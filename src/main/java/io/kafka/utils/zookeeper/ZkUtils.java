package io.kafka.utils.zookeeper;


import io.kafka.cluster.Broker;
import io.kafka.cluster.Cluster;
import io.kafka.consumer.TopicCount;
import io.kafka.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.github.zkclient.ZkClient;
import com.github.zkclient.exception.ZkNoNodeException;
import com.github.zkclient.exception.ZkNodeExistsException;

/**
 * @author tf
 * @version 创建时间：2019年1月29日 下午5:17:47
 * @ClassName ZkUtils
 */
public class ZkUtils {

    public static final String ConsumersPath = "/consumers";

    public static final String BrokerIdsPath = "/brokers/ids";

    public static final String BrokerTopicsPath = "/brokers/topics";


    public static void makeSurePersistentPathExists(ZkClient zkClient, String path) {
        if (!zkClient.exists(path)) {
            zkClient.createPersistent(path, true);
        }
    }

    /**
     * get children nodes name
     *
     * @param zkClient zkClient
     * @param path     full path
     * @return children nodes name or null while path not exist
     */
    public static List<String> getChildrenParentMayNotExist(ZkClient zkClient, String path) {
        try {
            return zkClient.getChildren(path);
        } catch (ZkNoNodeException e) {
            return null;
        }
    }

    public static String readData(ZkClient zkClient, String path) {
        return Utils.fromBytes(zkClient.readData(path));
    }

    public static String readDataMaybeNull(ZkClient zkClient, String path) {
        return Utils.fromBytes(zkClient.readData(path, true));
    }

    public static void updatePersistentPath(ZkClient zkClient, String path, String data) {
        try {
            zkClient.writeData(path, Utils.getBytes(data));
        } catch (ZkNoNodeException e) {
            createParentPath(zkClient, path);
            try {
                zkClient.createPersistent(path, Utils.getBytes(data));
            } catch (ZkNodeExistsException e2) {
                zkClient.writeData(path, Utils.getBytes(data));
            }
        }
    }

    private static void createParentPath(ZkClient zkClient, String path) {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0) {
            zkClient.createPersistent(parentDir, true);
        }
    }

    /**
     * read all brokers in the zookeeper
     *
     * @param zkClient zookeeper client
     * @return all brokers
     */
//    public static Cluster getCluster(ZkClient zkClient) {
//        Cluster cluster = new Cluster();
//        List<String> nodes = getChildrenParentMayNotExist(zkClient, BrokerIdsPath);
//        for (String node : nodes) {
//            final String brokerInfoString = readData(zkClient, BrokerIdsPath + "/" + node);
//            cluster.add(Broker.createBroker(Integer.valueOf(node), brokerInfoString));
//        }
//        return cluster;
//    }
//
//    public static TopicCount getTopicCount(ZkClient zkClient, String group, String consumerId) {
//        ZkGroupDirs dirs = new ZkGroupDirs(group);
//        String topicCountJson = ZkUtils.readData(zkClient, dirs.consumerRegistryDir + "/" + consumerId);
//        return TopicCount.parse(consumerId, topicCountJson);
//    }

    /**
     * read broker info topics
     *
     * @param zkClient the zookeeper client
     * @param topics   topic names
     * @return topic-&gt;(brokerid-0,brokerid-1...brokerid2-0,brokerid2-1...)
     */
    public static Map<String, List<String>> getPartitionsForTopics(ZkClient zkClient, Collection<String> topics) {
        Map<String, List<String>> ret = new HashMap<String, List<String>>();
        for (String topic : topics) {
            List<String> partList = new ArrayList<String>();
            List<String> brokers = getChildrenParentMayNotExist(zkClient, BrokerTopicsPath + "/" + topic);
            if (brokers != null) {
                for (String broker : brokers) {
                    final String parts = readData(zkClient, BrokerTopicsPath + "/" + topic + "/" + broker);
                    int nParts = Integer.parseInt(parts);
                    for (int i = 0; i < nParts; i++) {
                        partList.add(broker + "-" + i);
                    }
                }
            }
            Collections.sort(partList);
            ret.put(topic, partList);
        }
        return ret;
    }

    /**
     * get all consumers for the group
     *
     * @param zkClient the zookeeper client
     * @param group    the group name
     * @return topic-&gt;(consumerIdStringA-0,consumerIdStringA-1...consumerIdStringB-0,consumerIdStringB-1)
     */
//    public static Map<String, List<String>> getConsumersPerTopic(ZkClient zkClient, String group) {
//        ZkGroupDirs dirs = new ZkGroupDirs(group);
//        List<String> consumers = getChildrenParentMayNotExist(zkClient, dirs.consumerRegistryDir);
//        //
//        Map<String, List<String>> consumersPerTopicMap = new HashMap<String, List<String>>();
//        for (String consumer : consumers) {
//            TopicCount topicCount = getTopicCount(zkClient, group, consumer);
//            for (Map.Entry<String, Set<String>> e : topicCount.getConsumerThreadIdsPerTopic().entrySet()) {
//                final String topic = e.getKey();
//                for (String consumerThreadId : e.getValue()) {
//                    List<String> list = consumersPerTopicMap.get(topic);
//                    if (list == null) {
//                        list = new ArrayList<String>();
//                        consumersPerTopicMap.put(topic, list);
//                    }
//                    //
//                    list.add(consumerThreadId);
//                }
//            }
//        }
//        //
//        for (Map.Entry<String, List<String>> e : consumersPerTopicMap.entrySet()) {
//            Collections.sort(e.getValue());
//        }
//        return consumersPerTopicMap;
//    }
    //

    public static void deletePath(ZkClient zkClient, String path) {
        try {
            zkClient.delete(path);
        } catch (ZkNoNodeException e) {
        }
    }

    public static String readDataMaybyNull(ZkClient zkClient, String path) {
        return Utils.fromBytes(zkClient.readData(path, true));
    }

    /**
     * Create an ephemeral node with the given path and data. Create parents if necessary.
     * @param zkClient client of zookeeper
     * @param path node path of zookeeper
     * @param data node data
     */
    public static void createEphemeralPath(ZkClient zkClient, String path, String data) {
        try {
            zkClient.createEphemeral(path, Utils.getBytes(data));
        } catch (ZkNoNodeException e) {
            createParentPath(zkClient, path);
            zkClient.createEphemeral(path, Utils.getBytes(data));
        }
    }

    public static void createEphemeralPathExpectConflict(ZkClient zkClient, String path, String data) {
        try {
            createEphemeralPath(zkClient, path, data);
        } catch (ZkNodeExistsException e) {
            //this can happend when there is connection loss;
            //make sure the data is what we intend to write
            String storedData = null;
            try {
                storedData = readData(zkClient, path);
            } catch (ZkNoNodeException e2) {
                //ignore
            }
            if (storedData == null || !storedData.equals(data)) {
                throw new ZkNodeExistsException("conflict in " + path + " data: " + data + " stored data: " + storedData);
            }
            //
            //otherwise, the creation succeeded, return normally
        }
    }

    /**
     * 获取/brokers/ids的broker集合
     * @param zkClient
     * @return
     */
	public static Cluster getCluster(ZkClient zkClient) {
		Cluster cluster = new Cluster();
        List<String> nodes = getChildrenParentMayNotExist(zkClient, BrokerIdsPath);
        for (String node : nodes) {
            final String brokerInfoString = readData(zkClient, BrokerIdsPath + "/" + node);
            cluster.add(Broker.createBroker(Integer.valueOf(node), brokerInfoString));
        }
        return cluster;
	}

	public static TopicCount getTopicCount(ZkClient zkClient, String group, String consumerId) {
		ZkGroupDirs dirs = new ZkGroupDirs(group);
        String topicCountJson = ZkUtils.readData(zkClient, dirs.consumerRegistryDir + "/" + consumerId);
        return TopicCount.parse(consumerId, topicCountJson);
	}

	public static Map<String, List<String>> getConsumersPerTopic(ZkClient zkClient, String group) {
		ZkGroupDirs dirs = new ZkGroupDirs(group);
        List<String> consumers = getChildrenParentMayNotExist(zkClient, dirs.consumerRegistryDir);
        //
        Map<String, List<String>> consumersPerTopicMap = new HashMap<String, List<String>>();
        for (String consumer : consumers) {
            TopicCount topicCount = getTopicCount(zkClient, group, consumer);
            for (Map.Entry<String, Set<String>> e : topicCount.getConsumerThreadIdsPerTopic().entrySet()) {
                final String topic = e.getKey();
                for (String consumerThreadId : e.getValue()) {
                    List<String> list = consumersPerTopicMap.get(topic);
                    if (list == null) {
                        list = new ArrayList<String>();
                        consumersPerTopicMap.put(topic, list);
                    }
                    //
                    list.add(consumerThreadId);
                }
            }
        }
        //
        for (Map.Entry<String, List<String>> e : consumersPerTopicMap.entrySet()) {
            Collections.sort(e.getValue());
        }
	    return consumersPerTopicMap;
	}
}