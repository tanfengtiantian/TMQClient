package io.kafka.consumer;


import io.kafka.cluster.Cluster;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.zkclient.ZkClient;

/**
 * @author tf
 * @version 创建时间：2019年1月31日 下午3:41:11
 * @ClassName Fetcher
 */
public class Fetcher {

	private final ConsumerConfig config;
	
    private final ZkClient zkClient;

    private final Logger logger = LoggerFactory.getLogger(Fetcher.class);
    
    private volatile List<FetcherRunnable> fetcherThreads = new ArrayList<FetcherRunnable>(0);
    
	public Fetcher(ConsumerConfig config, ZkClient zkClient) {
		this.config = config;
        this.zkClient = zkClient;
	}
	
	public <T> void startConnections(Iterable<PartitionTopicInfo> topicInfos, Cluster cluster,//
            Map<String, List<MessageStream<T>>> messageStreams) {
		if (topicInfos == null) {
            return;
        }
		
		 //re-arrange by broker id
        Map<Integer, List<PartitionTopicInfo>> m = new HashMap<Integer, List<PartitionTopicInfo>>();
        for (PartitionTopicInfo info : topicInfos) {
        	 if (cluster.getBroker(info.brokerId) == null) {
                 throw new IllegalStateException("Broker " + info.brokerId + " is unavailable, fetchers could not be started");
             }
             List<PartitionTopicInfo> list = m.get(info.brokerId);
             if (list == null) {
                 list = new ArrayList<PartitionTopicInfo>();
                 m.put(info.brokerId, list);
             }
             list.add(info);
        }
        //
        final List<FetcherRunnable> fetcherThreads = new ArrayList<FetcherRunnable>();
        for (Map.Entry<Integer, List<PartitionTopicInfo>> e : m.entrySet()) {
            FetcherRunnable fetcherThread = new FetcherRunnable("FetchRunnable-" + e.getKey(), //
                    zkClient, //
                    config, //
                    cluster.getBroker(e.getKey()), //
                    e.getValue());
            fetcherThreads.add(fetcherThread);
            fetcherThread.start();
        }
        //
        this.fetcherThreads = fetcherThreads;
	}

	public void stopConnectionsToAllBrokers() {
		List<FetcherRunnable> threads = this.fetcherThreads;
        this.fetcherThreads = new ArrayList<FetcherRunnable>(0);
        for (FetcherRunnable fetcherThread : threads) {
            try {
                fetcherThread.shutdown();
            } catch (InterruptedException e) {
                logger.warn(e.getMessage(), e);
            }
        }
	}
	
	public <T> void clearFetcherQueues(Collection<BlockingQueue<FetchedDataChunk>> queuesToBeCleared, Collection<List<MessageStream<T>>> messageStreamsList) {
        for (BlockingQueue<FetchedDataChunk> q : queuesToBeCleared) {
            q.clear();
        }
        //
        for (List<MessageStream<T>> messageStreams : messageStreamsList) {
            for (MessageStream<T> ms : messageStreams) {
                ms.clear();
            }
        }
    }

}
