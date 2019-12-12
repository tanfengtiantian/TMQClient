package io.kafka.producer;


import io.kafka.api.ProducerRequest;
import io.kafka.cluster.Broker;
import io.kafka.cluster.Partition;
import io.kafka.common.exception.InvalidPartitionException;
import io.kafka.common.exception.NoBrokersForPartitionException;
import io.kafka.common.exception.TransactionInProgressException;
import io.kafka.producer.async.CallbackHandler;
import io.kafka.producer.async.DefaultEventHandler;
import io.kafka.producer.async.EventHandler;
import io.kafka.producer.config.ConfigBrokerPartitionInfo;
import io.kafka.producer.config.ProducerConfig;
import io.kafka.producer.data.ProducerData;
import io.kafka.producer.data.ProducerPoolData;
import io.kafka.producer.pool.ProducerPool;
import io.kafka.producer.serializer.Encoder;
import io.kafka.transaction.ITransactionProducer;
import io.kafka.transaction.TransactionContext;
import io.kafka.utils.Closer;
import io.kafka.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tf
 * @version 创建时间：2019年1月19日 上午11:46:21
 * @ClassName Message producer
 */
public class Producer<K, V> implements BrokerPartitionInfo.Callback, ITransactionProducer ,IProducer<K, V> {

	private ProducerConfig config;

	private IPartitioner<K> partitioner;

	private final Logger logger = LoggerFactory.getLogger(Producer.class);
	/**
	 * 生产池
	 */
	private ProducerPool<V> producerPool;
	
	private boolean populateProducerPool;
	
	private BrokerPartitionInfo brokerPartitionInfo;
	
	private final Random random = new Random();
	
	private Encoder<V> encoder;
	
	private DefaultPartitioner<K> defaultPartitioner =new DefaultPartitioner<>();
	
	/////////////////////////////////////////////////////////////////////////
	private final AtomicBoolean hasShutdown = new AtomicBoolean(false);

	public Producer(ProducerConfig config, IPartitioner<K> partitioner, ProducerPool<V> producerPool, boolean populateProducerPool,
             BrokerPartitionInfo brokerPartitionInfo,boolean sync) {
		super();
        this.config = config;
        if (producerPool == null && sync) {
            producerPool = new ProducerPool<V>(this,config, getEncoder());
        }
        this.partitioner = partitioner;
        this.producerPool = producerPool;
        this.populateProducerPool = populateProducerPool;
        this.brokerPartitionInfo = brokerPartitionInfo;
        if (this.brokerPartitionInfo == null) {
        	this.brokerPartitionInfo = new ConfigBrokerPartitionInfo(config);
        }

	}

	/**
	 * async
	 * @param config
	 * @param eventHandler
	 * @param cbkHandler
	 */
	public Producer(ProducerConfig config,EventHandler<V> eventHandler, CallbackHandler<V> cbkHandler) {
		this(config, //
				null,//
				null, //
				true, //
				null,false);
		this.producerPool = new ProducerPool<V>(this ,
				config,
				getEncoder(),
				null,
				new ConcurrentHashMap<>(),
				eventHandler, cbkHandler,false);
		// pool -  broker
		if (this.populateProducerPool) {
			for (Map.Entry<Integer, Broker> e : this.brokerPartitionInfo.getAllBrokerInfo().entrySet()) {
				Broker b = e.getValue();
				producerPool.addProducer(new Broker(e.getKey(), b.host, b.host, b.port,b.autocreated));
			}
		}
	}

	/**
	 * sync
	 * @param config
	 */
	public Producer(ProducerConfig config) {
        this(config, //
                null,//
                null, //
                true, //
                null,true);
		// pool -  broker
		if (this.populateProducerPool) {
			for (Map.Entry<Integer, Broker> e : this.brokerPartitionInfo.getAllBrokerInfo().entrySet()) {
				Broker b = e.getValue();
				producerPool.addProducer(new Broker(e.getKey(), b.host, b.host, b.port,b.autocreated));
			}
		}
    }
	
	@Override
	public SendResult send(ProducerData<K, V> data)
			throws Exception{
		if (data == null) return null;
		return configSend(data);
	}

	/**
	 * 集群仅支持固定分区,无法感知集群节点宕机
	 * @param data
	 */
	private SendResult configSend(ProducerData<K, V> data) throws Exception{
		return producerPool.send(create(data));
	}
	/**
	 * 选择broker-Partition
	 * @param pd
	 * @return
	 */
	private ProducerPoolData<V> create(ProducerData<K, V> pd) {
		//0-1,0-2,1-1,1-2
		Collection<Partition> topicPartitionsList = getPartitionListForTopic(pd);
		//hashCode   broker
		int hashCodeBrokerId = getPartition(pd.getKey(), topicPartitionsList.size());
        final Partition brokerIdPartition = new ArrayList<>(topicPartitionsList).get(hashCodeBrokerId);
        return this.producerPool.getProducerPoolData(pd.getTopic(),//brokerIdPartition.partId
                new Partition(brokerIdPartition.brokerId, ProducerRequest.RandomPartition), pd.getTtl(),pd.getData());
	}
	private Collection<Partition> getPartitionListForTopic(
			ProducerData<K, V> pd) {
		SortedSet<Partition> topicPartitionsList = brokerPartitionInfo.getBrokerPartitionInfo(pd.getTopic());
        if (topicPartitionsList.size() == 0) {
            throw new NoBrokersForPartitionException("Partition= " + pd.getTopic());
        }
        return topicPartitionsList;
	}
	
	private int getPartition(K key, int numPartitions) {
        if (numPartitions <= 0) {
            throw new InvalidPartitionException("numPartitions 配置必须 > 0");
        }
        int partition = key == null ? random.nextInt(numPartitions) : getPartitioner().partition(key, numPartitions);
        if (partition < 0 || partition >= numPartitions) {
            throw new InvalidPartitionException("partition id : [" + partition + "]\n 无效,不在numPartitions[" + numPartitions + "]范围内.");
        }
        return partition;
    }
	
	@SuppressWarnings("unchecked")
	@Override
	public Encoder<V> getEncoder() {
		return encoder == null ? (Encoder<V>) Utils.getObject(config.getSerializerClass()) : encoder;
	}

	@Override
	public IPartitioner<K> getPartitioner() {
		return defaultPartitioner;
	}

	@Override
	public void producerCbk(int bid, String host, int port, boolean autocreated) {
		
	}
	
	@Override
	public void close() throws IOException {
		if (hasShutdown.compareAndSet(false, true)) {
            Closer.closeQuietly(producerPool);
            Closer.closeQuietly(brokerPartitionInfo);
        }
	}
	/**--------------------------------------与线程关联的事务上下文-----------------------------------**/

	/**
	 * 与线程关联的事务上下文
	 */
	final protected ThreadLocal<TransactionContext> transactionContext = new ThreadLocal<TransactionContext>();
	/**
	 * 事务相关代码
	 *
	 * @author tf
	 * @date 2019-07-17
	 */
	// 记录事务内上一次发送消息的信息，主要是为了记录第一次发送消息的serverUrl
	protected final ThreadLocal<Partition> lastSentInfo = new ThreadLocal<Partition>();

	// 默认事务超时时间为10秒
	protected volatile int transactionTimeout = 10;
	//单次请求最大超时毫秒单位
	protected long transactionRequestTimeoutInMills = 5000;

    @Override
    public void beginTransaction() throws Exception {
        // 没有在此方法里调用begin，而是等到第一次发送消息前才调用
        TransactionContext ctx = this.transactionContext.get();
        if (ctx == null) {
            ctx = new TransactionContext(transactionTimeout,transactionRequestTimeoutInMills);
            this.transactionContext.set(ctx);
        }else {
            throw new TransactionInProgressException("A transaction has begin");
        }
    }

	@Override
	public void setTransactionTimeout(int seconds) throws Exception {
		if (seconds < 0) {
			throw new IllegalArgumentException("Illegal transaction timeout value");
		}
		this.transactionTimeout = seconds;
	}

	@Override
	public int getTransactionTimeout() throws Exception {
		return transactionTimeout;
	}

	@Override
	public void rollback() throws Exception{
		try {
			final TransactionContext ctx = this.getTx();
			if(ctx == null){
				throw new Exception("rollback=>TransactionContext is null");
			}
			ctx.rollback();
		}
		finally {
			this.resetLastSentInfo();
			this.transactionContext.remove();
		}
	}

	@Override
	public void commit() throws Exception{
		try {
			final TransactionContext ctx = this.getTx();
			if(ctx == null){
				throw new Exception("commit=>TransactionContext is null");
			}
			ctx.commit();
		}
		finally {
			this.resetLastSentInfo();
			this.transactionContext.remove();
		}

	}

	protected void resetLastSentInfo() {
		this.lastSentInfo.remove();
	}

	@Override
	public Partition getPartitionSelector() {
		return null;
	}

	/**
	 * 判断是否处于事务中
	 *
	 * @return
	 */
	public boolean isInTransaction() {
		return this.transactionContext.get() != null;
	}

	@Override
	public TransactionContext getTx() {
		return this.transactionContext.get();
	}

	@Override
	public Partition getLastSentInfo() {
		return lastSentInfo.get();
	}

	@Override
	public void setLastSentInfo(Partition part) {
		if (this.isInTransaction() && this.lastSentInfo.get() == null) {
			this.lastSentInfo.set(part);
		}
	}
}
