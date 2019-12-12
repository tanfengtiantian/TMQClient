package io.kafka.consumer;

import static java.lang.String.format;
import io.kafka.producer.serializer.Decoder;
import io.kafka.utils.Closer;
import io.kafka.utils.KV.StringTuple;
import io.kafka.utils.Pool;
import io.kafka.utils.Scheduler;
import io.kafka.utils.zookeeper.ZkGroupDirs;
import io.kafka.utils.zookeeper.ZkGroupTopicDirs;
import io.kafka.utils.zookeeper.ZkUtils;
import io.kafka.api.OffsetRequest;
import io.kafka.cluster.Broker;
import io.kafka.cluster.Cluster;
import io.kafka.cluster.Partition;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.zkclient.IZkChildListener;
import com.github.zkclient.IZkStateListener;
import com.github.zkclient.ZkClient;
import com.github.zkclient.exception.ZkNoNodeException;
import com.github.zkclient.exception.ZkNodeExistsException;

/**
 * @author tf
 * @version 创建时间：2019年1月31日 下午2:51:17
 * @ClassName ZookeeperConsumerConnector
 */
public class ZookeeperConsumerConnector implements ConsumerConnector {

	static final FetchedDataChunk SHUTDOWN_COMMAND = new FetchedDataChunk(null, null, -1);
	
	private final Logger logger = LoggerFactory.getLogger(ZookeeperConsumerConnector.class);
	
	private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
	
	private final Object rebalanceLock = new Object();
	 
	private final ConsumerConfig config;
	
	private final boolean enableFetcher;
	
	private Pool<String, Pool<Partition, PartitionTopicInfo>> topicRegistry;
	
	private final Pool<StringTuple, BlockingQueue<FetchedDataChunk>> queues;
	
	private final Scheduler scheduler = new Scheduler(1, "consumer-autocommit-", false);
	 
	private ZkClient zkClient;

	private Fetcher fetcher;
	
	//cache for shutdown
    private List<ZKRebalancerListener<?>> rebalancerListeners = new ArrayList<ZKRebalancerListener<?>>();
	
	public ZookeeperConsumerConnector(ConsumerConfig config) {
		this(config, true);
	}

	public ZookeeperConsumerConnector(ConsumerConfig config, boolean enableFetcher) {
		this.config = config;
        this.enableFetcher = enableFetcher;
        //
        this.topicRegistry = new Pool<String, Pool<Partition, PartitionTopicInfo>>();
        this.queues = new Pool<StringTuple, BlockingQueue<FetchedDataChunk>>();
        //
        connectZk();
        createFetcher();
        //开启自动提交 
        if (this.config.isAutoCommit()) {
            logger.info("start auto committer " + config.getAutoCommitIntervalMs() + " ms");
            scheduler.scheduleWithRate(new AutoCommitTask(), config.getAutoCommitIntervalMs(),
                    config.getAutoCommitIntervalMs());
        }
	}
	
	private void connectZk() {
        logger.debug("Connecting zookeeper instance " + config.getZkConnect());
        this.zkClient = new ZkClient(config.getZkConnect(), config.getZkSessionTimeoutMs(),
                config.getZkConnectionTimeoutMs());
    }
	
	/**
    *
    */
   private void createFetcher() {
       if (enableFetcher) {
           this.fetcher = new Fetcher(config, zkClient);
       }
   }

	class AutoCommitTask implements Runnable {
        public void run() {
            try {
                commitOffsets();
            } catch (Throwable e) {
                logger.error("autoCommit error: ", e);
            }
        }
    }

	@Override
	public <T> Map<String, List<MessageStream<T>>> createMessageStreams(
			Map<String, Integer> topicCountMap, Decoder<T> decoder) {
		if (topicCountMap == null) {
            throw new IllegalArgumentException("topicCountMap is null");
        }
		//
        ZkGroupDirs dirs = new ZkGroupDirs(config.getGroupId());
        Map<String, List<MessageStream<T>>> ret = new HashMap<String, List<MessageStream<T>>>();
        String consumerUuid = config.getConsumerId();
        if (consumerUuid == null) {
            consumerUuid = generateConsumerId();
        }
        logger.info(format("create message stream by consumerid [%s] with groupid [%s]", consumerUuid,
                config.getGroupId()));
        //consumerIdString => groupid_consumerid
        final String consumerIdString = config.getGroupId() + "_" + consumerUuid;
        final TopicCount topicCount = new TopicCount(consumerIdString, topicCountMap);
        for (Map.Entry<String, Set<String>> e : topicCount.getConsumerThreadIdsPerTopic().entrySet()) {
        	 final String topic = e.getKey();
             final Set<String> threadIdSet = e.getValue();
             final List<MessageStream<T>> streamList = new ArrayList<MessageStream<T>>();
             for (String threadId : threadIdSet) {
            	 //最大消费数量
            	 LinkedBlockingQueue<FetchedDataChunk> stream = new LinkedBlockingQueue<FetchedDataChunk>(
                         config.getMaxQueuedChunks());
            	 queues.put(new StringTuple(topic, threadId), stream);
                 streamList.add(new MessageStream<T>(topic, stream, config.getConsumerTimeoutMs(), decoder));
             }
             ret.put(topic, streamList);
             logger.debug("add topic [" + topic + "] and stream map.");
        }
        
        //侦听使用者和分区更改
        ZKRebalancerListener<T> loadBalancerListener = new ZKRebalancerListener<T>(config.getGroupId(),dirs,topicCount, consumerIdString, ret);
        this.rebalancerListeners.add(loadBalancerListener);
        // 首次注册 consumer path: /consumers/groupid/ids/groupid-consumerid data: {topic:count,topic:count} 
        loadBalancerListener.registerConsumer();
        
        //为会话过期事件注册侦听器
        zkClient.subscribeStateChanges(loadBalancerListener);
        //'/consumers/group/ids' 状态监听
        zkClient.subscribeChildChanges(dirs.consumerRegistryDir, loadBalancerListener);
        // start thread
        loadBalancerListener.start();
        
        for (String topic : ret.keySet()) {
            final String partitionPath = ZkUtils.BrokerTopicsPath + "/" + topic;
            // 监听/brokers/topics/topic  状态更改
            zkClient.subscribeChildChanges(partitionPath, loadBalancerListener);
        }
        //consumer 负载均衡
        //注册path: /consumers/groupid/owners/demo1
        loadBalancerListener.syncedRebalance();
        return ret;
	}

	private String generateConsumerId() {
		UUID uuid = UUID.randomUUID();
        try {
            return format("%s-%d-%s", InetAddress.getLocalHost().getHostName(), //
                    System.currentTimeMillis(),//
                    Long.toHexString(uuid.getMostSignificantBits()).substring(0, 8));
        } catch (UnknownHostException e) {
            try {
                return format("%s-%d-%s", InetAddress.getLocalHost().getHostAddress(), //
                        System.currentTimeMillis(),//
                        Long.toHexString(uuid.getMostSignificantBits()).substring(0, 8));
            } catch (UnknownHostException ex) {
                throw new IllegalArgumentException(
                        "'consumerid' 生成失败");
            }
        }
	}

	@Override
	public void commitOffsets() {
		 if (zkClient == null) {
	            logger.error("zk client is null. Cannot commit offsets");
	            return;
	        }
	        for (Entry<String, Pool<Partition, PartitionTopicInfo>> e : topicRegistry.entrySet()) {
	            ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(config.getGroupId(), e.getKey());
	            //
	            for (PartitionTopicInfo info : e.getValue().values()) {
	                final long lastChanged = info.getConsumedOffsetChanged().get();
	                if (lastChanged == 0) {
	                    logger.trace("consume offset not changed");
	                    continue;
	                }
	                final long newOffset = info.getConsumedOffset();
	                //path: /consumers/<group>/offsets/<topic>/<brokerid-partition>
	                final String path = topicDirs.consumerOffsetDir + "/" + info.partition.getName();
	                try {
	                    ZkUtils.updatePersistentPath(zkClient, path, "" + newOffset);
	                } catch (Throwable t) {
	                    logger.warn("exception during commitOffsets, path=" + path + ",offset=" + newOffset, t);
	                } finally {
	                    info.resetComsumedOffsetChanged(lastChanged);
	                    if (logger.isDebugEnabled()) {
	                        logger.debug("Committed [" + path + "] for topic " + info);
	                    }
	                }
	            }
	            //
	        }
	}

	@Override
	public void close() throws IOException {
        if (isShuttingDown.compareAndSet(false, true)) {
            logger.info("ZkConsumerConnector shutting down");
            try {
                scheduler.shutdown();
                if (fetcher != null) {
                    fetcher.stopConnectionsToAllBrokers();
                }
                sendShutdownToAllQueues();
                if (config.isAutoCommit()) {
                    commitOffsets();
                }
                for (ZKRebalancerListener<?> listener : this.rebalancerListeners) {
                    Closer.closeQuietly(listener);
                }
                if (this.zkClient != null) {
                    this.zkClient.close();
                    zkClient = null;
                }
            } catch (Exception e) {
                logger.error("error consumer shutdown", e);
            }
            logger.info("ZkConsumerConnector completed");
        }
    }
	private void sendShutdownToAllQueues() {
        for (BlockingQueue<FetchedDataChunk> queue : queues.values()) {
            queue.clear();
            try {
                queue.put(SHUTDOWN_COMMAND);
            } catch (InterruptedException e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }
	
	
	class ZKRebalancerListener<T> implements IZkChildListener, IZkStateListener , Runnable, Closeable {

        final String group;
        final TopicCount topicCount;
        final ZkGroupDirs zkGroupDirs;

        final String consumerIdString;

        Map<String, List<MessageStream<T>>> messagesStreams;

        //
        private boolean isWatcherTriggered = false;

        private final ReentrantLock lock = new ReentrantLock();

        private final Condition cond = lock.newCondition();

        private final Thread watcherExecutorThread;

        private CountDownLatch shutDownLatch = new CountDownLatch(1);

        public ZKRebalancerListener(String group, ZkGroupDirs zkGroupDirs, TopicCount topicCount, String consumerIdString,
                                    Map<String, List<MessageStream<T>>> messagesStreams) {
            this.group = group;
            this.zkGroupDirs = zkGroupDirs;
            this.topicCount = topicCount;
            this.consumerIdString = consumerIdString;
            this.messagesStreams = messagesStreams;
            //
            this.watcherExecutorThread = new Thread(this, consumerIdString + "_watcher_executor");
        }

        public void start() {
            this.watcherExecutorThread.start();
        }

        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            lock.lock();
            try {
                logger.info("handle consumer changed: group={} consumerId={} parentPath={} currentChilds={}",
                        group,consumerIdString,parentPath,currentChilds);
                isWatcherTriggered = true;
                cond.signalAll();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            lock.lock();
            try {
                isWatcherTriggered = false;
                cond.signalAll();
            } finally {
                lock.unlock();
            }
            try {
                shutDownLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                //ignore
            }
        }

        public void run() {
            logger.info("starting executor thread consumer " + consumerIdString);
            boolean doRebalance;
            while (!isShuttingDown.get()) {
                try {
                    lock.lock();
                    try {
                        if (!isWatcherTriggered) {
                            cond.await(1000, TimeUnit.MILLISECONDS);
                        }
                    } finally {
                        doRebalance = isWatcherTriggered;
                        isWatcherTriggered = false;
                        lock.unlock();
                    }
                    if (doRebalance) {
                        syncedRebalance();
                    }
                } catch (Throwable t) {
                    logger.error("error syncedRebalance", t);
                }
            }
            //
            logger.info("stopped thread " + watcherExecutorThread.getName());
            shutDownLatch.countDown();
        }

        private void syncedRebalance() {
            synchronized (rebalanceLock) {
                for (int i = 0; i < config.getMaxRebalanceRetries(); i++) {
                    if (isShuttingDown.get()) {//do nothing while shutting down
                        return;
                    }
                    logger.info(format("[%s] rebalancing starting. try #%d", consumerIdString, i));
                    final long start = System.currentTimeMillis();
                    boolean done = false;
                    Cluster cluster = ZkUtils.getCluster(zkClient);
                    try {
                        done = rebalance(cluster);
                    } catch (ZkNoNodeException znne){
                        logger.info("some consumers dispeared during rebalancing: {}",znne.getMessage());
                        registerConsumer();
                    }
                    catch (Exception e) {
                        logger.info("重新平衡期间出现异常： ", e);
                    }
                    logger.info(format("[%s] rebalanced %s. try #%d, cost %d ms",//
                            consumerIdString, done ? "OK" : "FAILED", i, System.currentTimeMillis() - start));
                    //
                    if (done) {
                        return;
                    } else {
                        /* 在这里，缓存可能会过时。为了正确地做出未来的再平衡决策，我们应该清除缓存 */
                    }
                    //
                    closeFetchersForQueues( messagesStreams, queues.values());
                    try {
                        Thread.sleep(config.getRebalanceBackoffMs());
                    } catch (InterruptedException e) {
                        logger.warn(e.getMessage());
                    }
                }
            }
            throw new RuntimeException(
                    consumerIdString + " can't rebalance after " + config.getMaxRebalanceRetries() + " retries");
        }

        private boolean rebalance(Cluster cluster) {
            // map for current consumer: topic->[ groupid-consumer-0, groupid-consumer-1 ,..., groupid-consumer-N]
        	//{demo1=[test_group_tanfeng-1549004771825-29a9630e-0, test_group_tanfeng-1549004771825-29a9630e-1]}
            Map<String, Set<String>> myTopicThreadIdsMap = ZkUtils.getTopicCount(zkClient, group, consumerIdString).getConsumerThreadIdsPerTopic();
            // map for all consumers in this group: topic->[groupid-consumer1-0,...,groupid-consumerX-N]
            //{demo1=[test_group_tanfeng-1549004771825-29a9630e-0, test_group_tanfeng-1549004771825-29a9630e-1]}
            Map<String, List<String>> consumersPerTopicMap = ZkUtils.getConsumersPerTopic(zkClient, group);
            // map for all broker-partitions for the topics in this consumerid: topic->[brokerid0-partition0,...,brokeridN-partitionN]
            //{demo1=[0-0, 0-1, 0-2, 0-3, 0-4, 1-0, 1-1, 1-2, 1-3, 1-4, 2-0, 2-1, 2-2, 2-3, 2-4]}
            Map<String, List<String>> brokerPartitionsPerTopicMap = ZkUtils.getPartitionsForTopics(zkClient,
                    myTopicThreadIdsMap.keySet());
            /*
             * 必须停止获取器以避免数据重复，因为如果当前
             * 重新平衡尝试失败，释放的分区可能属于
             * 另一个消费者。但是如果我们不先停止牵线器，这个消费者会
             * 继续并行返回已释放分区的数据。所以，不要停下来
             * 取数器导致重复数据。
             */
            closeFetchers(messagesStreams, myTopicThreadIdsMap);
            releasePartitionOwnership(topicRegistry);
            //
            Map<StringTuple, String> partitionOwnershipDecision = new HashMap<StringTuple, String>();
            Pool<String, Pool<Partition, PartitionTopicInfo>> currentTopicRegistry = new Pool<String, Pool<Partition, PartitionTopicInfo>>();
            for (Map.Entry<String, Set<String>> e : myTopicThreadIdsMap.entrySet()) {
                final String topic = e.getKey();
                currentTopicRegistry.put(topic, new Pool<Partition, PartitionTopicInfo>());
                //
                ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(group, topic);
                List<String> curConsumers = consumersPerTopicMap.get(topic);
                List<String> curBrokerPartitions = brokerPartitionsPerTopicMap.get(topic);

                final int nPartsPerConsumer = curBrokerPartitions.size() / curConsumers.size();
                final int nConsumersWithExtraPart = curBrokerPartitions.size() % curConsumers.size();
                logger.info("Consumer {} 重新平衡以下各项 {} partitions  topic {}:\n\t{}\n\twith {} consumers:\n\t{}",//
                        consumerIdString,curBrokerPartitions.size(),topic,curBrokerPartitions,curConsumers.size(),curConsumers
                        );
                if (logger.isDebugEnabled()) {
                    StringBuilder buf = new StringBuilder(1024);
                    buf.append("[").append(topic).append("] 预分配详细信息:");
                    for (int i = 0; i < curConsumers.size(); i++) {
                        final int startPart = nPartsPerConsumer * i + Math.min(i, nConsumersWithExtraPart);
                        final int nParts = nPartsPerConsumer + ((i + 1 > nConsumersWithExtraPart) ? 0 : 1);
                        if (nParts > 0) {
                            for (int m = startPart; m < startPart + nParts; m++) {
                                buf.append("\n    ").append(curConsumers.get(i)).append(" ==> ")
                                        .append(curBrokerPartitions.get(m));
                            }
                        }
                    }
                    logger.debug(buf.toString());
                }
                //consumerThreadId=> groupid_consumerid-index (index from count)
                for (String consumerThreadId : e.getValue()) {
                    final int myConsumerPosition = curConsumers.indexOf(consumerThreadId);
                    assert (myConsumerPosition >= 0);
                    final int startPart = nPartsPerConsumer * myConsumerPosition + Math.min(myConsumerPosition,
                            nConsumersWithExtraPart);
                    final int nParts = nPartsPerConsumer + ((myConsumerPosition + 1 > nConsumersWithExtraPart) ? 0 : 1);

                    if (nParts <= 0) {
                        logger.warn("No broker partition of topic {} for consumer {}, {} partitions and {} consumers",topic,consumerThreadId,//
                                curBrokerPartitions.size(),curConsumers.size());
                    } else {
                    	//brokerPartition 切分
                        for (int i = startPart; i < startPart + nParts; i++) {
                            String brokerPartition = curBrokerPartitions.get(i);
                            logger.info("[" + consumerThreadId + "] ==> " + brokerPartition + " claimming");
                            addPartitionTopicInfo(currentTopicRegistry, topicDirs, brokerPartition, topic,
                                    consumerThreadId);
                            // 记录分区所有权
                            partitionOwnershipDecision.put(new StringTuple(topic, brokerPartition), consumerThreadId);
                        }
                    }
                }
            }
            //
            /*
             * 分区所有权移到这里，因为这可以用来表示
             * 重新平衡尝试成功重新平衡尝试成功完成
             * 只有在回卷器正确启动后
             */
            if (reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
                logger.debug("Updating the cache");
                logger.debug("Partitions per topic cache " + brokerPartitionsPerTopicMap);
                logger.debug("Consumers per topic cache " + consumersPerTopicMap);
                topicRegistry = currentTopicRegistry;
                updateFetcher(cluster, messagesStreams);
                return true;
            } else {
                return false;
            }
            ////////////////////////////
        }

        private void updateFetcher(Cluster cluster, Map<String, List<MessageStream<T>>> messagesStreams2) {
            if (fetcher != null) {
                List<PartitionTopicInfo> allPartitionInfos = new ArrayList<PartitionTopicInfo>();
                for (Pool<Partition, PartitionTopicInfo> p : topicRegistry.values()) {
                    allPartitionInfos.addAll(p.values());
                }
                fetcher.startConnections(allPartitionInfos, cluster, messagesStreams2);
            }
        }

        private boolean reflectPartitionOwnershipDecision(Map<StringTuple, String> partitionOwnershipDecision) {
            final List<StringTuple> successfullyOwnerdPartitions = new ArrayList<StringTuple>();
            int hasPartitionOwnershipFailed = 0;
            for (Map.Entry<StringTuple, String> e : partitionOwnershipDecision.entrySet()) {
                final String topic = e.getKey().k;
                final String brokerPartition = e.getKey().v;
                final String consumerThreadId = e.getValue();
                final ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(group, topic);
                final String partitionOwnerPath = topicDirs.consumerOwnerDir + "/" + brokerPartition;
                try {
                    ZkUtils.createEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId);
                    successfullyOwnerdPartitions.add(new StringTuple(topic, brokerPartition));
                } catch (ZkNodeExistsException e2) {
                    logger.warn(format("[%s] waiting [%s] to release => %s",//
                            consumerThreadId,//
                            ZkUtils.readDataMaybeNull(zkClient, partitionOwnerPath),//
                            brokerPartition));
                    hasPartitionOwnershipFailed++;
                }
            }
            //
            if (hasPartitionOwnershipFailed > 0) {
                for (StringTuple topicAndPartition : successfullyOwnerdPartitions) {
                    deletePartitionOwnershipFromZK(topicAndPartition.k, topicAndPartition.v);
                }
                return false;
            }
            return true;
        }

        private void addPartitionTopicInfo(Pool<String, Pool<Partition, PartitionTopicInfo>> currentTopicRegistry,
                                           ZkGroupTopicDirs topicDirs, String brokerPartition, String topic, String consumerThreadId) {
            Partition partition = Partition.parse(brokerPartition);
            Pool<Partition, PartitionTopicInfo> partTopicInfoMap = currentTopicRegistry.get(topic);

            //consumers/test_group/offsets/demo1/1-3
            final String znode = topicDirs.consumerOffsetDir + "/" + partition.getName();
            String offsetString = ZkUtils.readDataMaybeNull(zkClient, znode);
            // 如果第一次启动使用者，请根据配置设置初始偏移量
            long offset;
            if (offsetString == null) {
                if (OffsetRequest.SMALLES_TIME_STRING.equals(config.getAutoOffsetReset())) {
                    offset = earliestOrLatestOffset(topic, partition.brokerId, partition.partId,
                            OffsetRequest.EARLIES_TTIME);
                } else if (OffsetRequest.LARGEST_TIME_STRING.equals(config.getAutoOffsetReset())) {
                    offset = earliestOrLatestOffset(topic, partition.brokerId, partition.partId,
                            OffsetRequest.LATES_TTIME);
                } else {
                    throw new RuntimeException("ConsumerConfig->autoOffsetReset->值错误");
                }

            } else {
                offset = Long.parseLong(offsetString);
            }
            BlockingQueue<FetchedDataChunk> queue = queues.get(new StringTuple(topic, consumerThreadId));
            AtomicLong consumedOffset = new AtomicLong(offset);
            AtomicLong fetchedOffset = new AtomicLong(offset);
            PartitionTopicInfo partTopicInfo = new PartitionTopicInfo(topic,//
                    partition,//
                    queue,//
                    consumedOffset,//
                    fetchedOffset);//
            partTopicInfoMap.put(partition, partTopicInfo);
            logger.debug(partTopicInfo + " selected new offset " + offset);
        }

        private long earliestOrLatestOffset(String topic, int brokerId, int partitionId, long earliestOrLatest) {
            SimpleConsumer simpleConsumer = null;
            long producedOffset = -1;

            try {
                Cluster cluster = ZkUtils.getCluster(zkClient);
                Broker broker = cluster.getBroker(brokerId);
                if (broker == null) {
                    throw new IllegalStateException(
                            "Broker " + brokerId + " is not found");
                }
                //
                //using default value???
                simpleConsumer = new SimpleConsumer(broker.host, broker.port, config.getSocketTimeoutMs(),
                        config.getSocketBufferSize());
                long[] offsets = simpleConsumer.getOffsetsBefore(topic, partitionId, earliestOrLatest, 1);
                if (offsets.length > 0) {
                    producedOffset = offsets[0];
                }
            } catch (Exception e) {
                logger.error("error in earliestOrLatestOffset() ", e);
            } finally {
                if (simpleConsumer != null) {
                    Closer.closeQuietly(simpleConsumer);
                }
            }
            return producedOffset;
        }

        private void releasePartitionOwnership(Pool<String, Pool<Partition, PartitionTopicInfo>> localTopicRegistry) {
            if (!localTopicRegistry.isEmpty()) {
                logger.info("partition ownership => " + localTopicRegistry);
                for (Map.Entry<String, Pool<Partition, PartitionTopicInfo>> e : localTopicRegistry.entrySet()) {
                    for (Partition partition : e.getValue().keySet()) {
                        deletePartitionOwnershipFromZK(e.getKey(), partition);
                    }
                }
                localTopicRegistry.clear();//clear all
            }
        }

        private void deletePartitionOwnershipFromZK(String topic, String partitionStr) {
            ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(group, topic);
            final String znode = topicDirs.consumerOwnerDir + "/" + partitionStr;
            ZkUtils.deletePath(zkClient, znode);
            logger.debug("Consumer [" + consumerIdString + "] released " + znode);
        }

        private void deletePartitionOwnershipFromZK(String topic, Partition partition) {
            this.deletePartitionOwnershipFromZK(topic, partition.toString());
        }


        private void closeFetchers(Map<String, List<MessageStream<T>>> messagesStreams2,
                                   Map<String, Set<String>> myTopicThreadIdsMap) {
            // topicRegistry.values()
            List<BlockingQueue<FetchedDataChunk>> queuesToBeCleared = new ArrayList<BlockingQueue<FetchedDataChunk>>();
            for (Map.Entry<StringTuple, BlockingQueue<FetchedDataChunk>> e : queues.entrySet()) {
                if (myTopicThreadIdsMap.containsKey(e.getKey().k)) {
                    queuesToBeCleared.add(e.getValue());
                }
            }
            closeFetchersForQueues( messagesStreams2, queuesToBeCleared);
        }

        private void closeFetchersForQueues(Map<String, List<MessageStream<T>>> messageStreams,
                                            Collection<BlockingQueue<FetchedDataChunk>> queuesToBeCleared) {
            if (fetcher == null) {
                return;
            }
            fetcher.stopConnectionsToAllBrokers();
            fetcher.clearFetcherQueues(queuesToBeCleared, messageStreams.values());
            if (config.isAutoCommit()) {
                logger.info("Committing all offsets after clearing the fetcher queues");
                commitOffsets();
            }
        }

        private void resetState() {
            topicRegistry.clear();
        }
        //

        @Override
        public void handleNewSession() throws Exception {
            //在ZooKeeper会话过期并创建新会话后调用
            /*
             * 当前消费者并在消费者注册中心重新注册该消费者，以及
             * 触发重新平衡
             */
            logger.info("Zk expired; release old broker partition ownership; re-register consumer " + consumerIdString);
            this.resetState();
            this.registerConsumer();
            //显式触发此使用者的负载平衡
            this.syncedRebalance();
            
        }

        @Override
        public void handleStateChanged(KeeperState state) throws Exception {
            // nothing to do
        }
        ///////////////////////////////////////////////////////////
        /**
         * 注册consumer到zookeeper
         * <p>
         *     path: /consumers/groupid/ids/groupid-consumerid
         *     data: {topic:count,topic:count}
         * </p>
         */
        void registerConsumer(){
            final String path = zkGroupDirs.consumerRegistryDir + "/" + consumerIdString;
            final String data = topicCount.toJsonString();
            boolean ok = true;
            try {
                ZkUtils.createEphemeralPathExpectConflict(zkClient, path, data);
            }catch (Exception ex){
                ok = false;
            }
            logger.info(format("register consumer in zookeeper [%s] => [%s] success?=%s", path, data, ok));
        }
    }

}
