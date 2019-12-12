package io.kafka.producer.pool;

import io.kafka.api.TransactionRequest;
import io.kafka.cluster.Broker;
import io.kafka.cluster.Partition;
import io.kafka.common.exception.TransactionInProgressException;
import io.kafka.common.exception.UnavailableProducerException;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.message.Message;
import io.kafka.producer.SendResult;
import io.kafka.producer.SyncProducer;
import io.kafka.producer.async.AsyncProducer;
import io.kafka.producer.async.AsyncProducerConfig;
import io.kafka.producer.async.CallbackHandler;
import io.kafka.producer.async.EventHandler;
import io.kafka.producer.config.ProducerConfig;
import io.kafka.producer.config.SyncProducerConfig;
import io.kafka.producer.data.ProducerPoolData;
import io.kafka.producer.serializer.Encoder;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import io.kafka.transaction.ITransactionProducer;
import io.kafka.transaction.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tf
 * @version 创建时间：2019年1月19日 下午2:25:47
 * @ClassName Producer 生产池
 */
public class ProducerPool<V> implements Closeable{
	
	private final Logger logger = LoggerFactory.getLogger(ProducerPool.class);
	
	private boolean sync = true;
	
	private final Encoder<V> serializer;
	
	private final ProducerConfig config;

	private ConcurrentMap<Integer, SyncProducer> syncProducers;

	private final ITransactionProducer transactionProducer;

	//Async
    private final EventHandler<V> eventHandler;

    private final CallbackHandler<V> callbackHandler;

    private final ConcurrentMap<Integer, AsyncProducer<V>> asyncProducers;

	public ProducerPool(ITransactionProducer transactionProducer, ProducerConfig config, Encoder<V> serializer) {
	    this(transactionProducer, config, serializer,
                new ConcurrentHashMap<>(),
                null,
                null,
                null,
                true
                );
	}

	public ProducerPool(ITransactionProducer transactionProducer, ProducerConfig config, Encoder<V> serializer,
                        ConcurrentMap<Integer, SyncProducer> syncProducers,//
                        ConcurrentMap<Integer, AsyncProducer<V>> asyncProducers, //
                        EventHandler<V> eventHandler,CallbackHandler<V> callbackHandler,
                        boolean sync) {
        this.transactionProducer=transactionProducer;
        this.serializer = serializer;
        this.config = config;
        this.syncProducers = syncProducers;
	    this.asyncProducers = asyncProducers;
	    this.eventHandler = eventHandler;
	    this.callbackHandler = callbackHandler;
	    this.sync = sync;
    }

	public SendResult send(ProducerPoolData<V> ppd) throws Exception {
		if (logger.isDebugEnabled()) {
            logger.debug("send message: " + ppd);
        }
		if (sync) {
            Message[] messages = new Message[ppd.data.size()];
            int index = 0;
            for (V v : ppd.data) {
                messages[index] = serializer.toMessage(v);
                index++;
            }
            Partition part = null;
            // 如果在事务内，则使用上一次发送消息时选择的broker
            if(transactionProducer.isInTransaction()){
                final Partition info = transactionProducer.getLastSentInfo();
                if (info != null) {
                    part = info;
                }
            }

            if(part == null) {
                part = ppd.partition;
            }

            SyncProducer producer = syncProducers.get(part.brokerId);
            if (producer == null) {
                throw new UnavailableProducerException("没有找到brokerId： " + part.brokerId);
            }
            // 第一次发送，需要启动事务
            if (transactionProducer.isInTransaction() && transactionProducer.getLastSentInfo() == null) {
                this.beforeSendMessageFirstTime(producer);
            }
            // 如果在事务内，发送事务消息
            if(transactionProducer.isInTransaction()){
                ByteBufferMessageSet bbms = new ByteBufferMessageSet(config.getCompressionCodec(), messages);
                TransactionRequest request = new TransactionRequest(TransactionRequest.PutCommand,
                        transactionProducer.getTx().getTransactionId().getTransactionKey(),
                        ppd.topic, part.partId, bbms);
                return getSendResult(producer.sendTransactionPutCommand(request,null));
            }else{
                ByteBufferMessageSet bbms = new ByteBufferMessageSet(config.getCompressionCodec(), messages);
                //ProducerRequest request = new ProducerRequest(ppd.topic, part.partId, bbms);
                return producer.send(ppd.topic, part.partId,ppd.ttl, bbms);
            }

        } else {
            AsyncProducer<V> asyncProducer = asyncProducers.get(ppd.partition.brokerId);
            for (V v : ppd.data) {
                asyncProducer.send(ppd.topic, v, ppd.partition.partId);
            }
        }
		return null;
	}
    private SendResult getSendResult(SendResult re) throws Exception {
        if (re.isSuccess()){
            // 记录本次发送信息，仅用于事务消息
            transactionProducer.setLastSentInfo(re.getPartition());
        }else{
            transactionProducer.rollback();
        }
        return  re;
    }
    private void beforeSendMessageFirstTime(SyncProducer producer) {
        final TransactionContext tx =transactionProducer.getTx();
        if (tx == null) {
            throw new TransactionInProgressException("transaction begin");
        }
        // 本地事务，则需要设置serverUrl并begin
        if (tx.getTransactionId() == null) {
            tx.setProducer(producer);
            tx.begin();
        }
    }

    /**
     * 这构造并返回生产者池的请求对象
     * 
     * @param topic 
     * @param bidPid  broker id and partition id
     * @param data 
     * @return producer
     */
    public ProducerPoolData<V> getProducerPoolData(String topic, Partition bidPid, long ttl,List<V> data) {
        return new ProducerPoolData<V>(topic, bidPid, ttl, data);
    }

	public void addProducer(Broker broker) {
		Properties props = new Properties();
        props.put("host", broker.host);
        props.put("port", "" + broker.port);
        props.putAll(config.getProperties());
        if (sync) {
            SyncProducer producer = new SyncProducer(new SyncProducerConfig(props));
            logger.info("create sync producer broker id = " + broker.id + " at " + broker.host + ":" + broker.port);
            syncProducers.put(broker.id, producer);
        }else {
            AsyncProducer<V> producer = new AsyncProducer<V>(new AsyncProducerConfig(props),//
                    new SyncProducer(new SyncProducerConfig(props)),//
                    serializer,//
                    eventHandler,//
                    null,//
                    this.callbackHandler, //
                    null);
            producer.start();
            logger.info("Creating async producer for broker id = " + broker.id + " at " + broker.host + ":" + broker.port);
            asyncProducers.put(broker.id, producer);
        }
	}
	
	@Override
	public void close() throws IOException {
		logger.info("Close all sync producers");
        if (sync) {
            for (SyncProducer p : syncProducers.values()) {
                p.close();
            }
        } else {
        for (AsyncProducer<V> p : asyncProducers.values()) {
            p.close();
        }
    }
	}

}
