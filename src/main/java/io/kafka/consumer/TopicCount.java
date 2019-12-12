package io.kafka.consumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * @author tf
 * @version 创建时间：2019年1月31日 下午4:50:49
 * @ClassName TopicCount(消费线程数topic-threadCount)
 */
public class TopicCount {

	private final String consumerIdString;

    private final Map<String, Integer> topicCountMap;
    
    private static final ObjectMapper mapper = new ObjectMapper();
    
    /**
     * @param consumerIdString groupid-consumerid
     * @param topicCountMap map: topic-threadCount
     */
	public TopicCount(String consumerIdString,
			Map<String, Integer> topicCountMap) {
		this.consumerIdString = consumerIdString;
        this.topicCountMap = topicCountMap;
	}
	
	/**
     * 
     * @return topic-(consumerIdString-0,consumerIdString-1..)
     */
    public Map<String, Set<String>> getConsumerThreadIdsPerTopic() {
    	//top set线程集合
        Map<String, Set<String>> consumerThreadIdsPerTopicMap = new HashMap<String, Set<String>>();
        for (Map.Entry<String, Integer> e : topicCountMap.entrySet()) {
            Set<String> consumerSet = new HashSet<String>();
            final int nCounsumers = e.getValue().intValue();
            for (int i = 0; i < nCounsumers; i++) {
                consumerSet.add(consumerIdString + "-" + i);
            }
            consumerThreadIdsPerTopicMap.put(e.getKey(), consumerSet);
        }
        return consumerThreadIdsPerTopicMap;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((consumerIdString == null) ? 0 : consumerIdString.hashCode());
        result = prime * result + ((topicCountMap == null) ? 0 : topicCountMap.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        TopicCount other = (TopicCount) obj;
        if (consumerIdString == null) {
            if (other.consumerIdString != null) return false;
        } else if (!consumerIdString.equals(other.consumerIdString)) return false;
        if (topicCountMap == null) {
            if (other.topicCountMap != null) return false;
        } else if (!topicCountMap.equals(other.topicCountMap)) return false;
        return true;
    }

    public String toJsonString() {
        try {
            return mapper.writeValueAsString(topicCountMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static TopicCount parse(String consumerIdString, String jsonString) {
        try {
            Map<String, Integer> topicCountMap = mapper.readValue(jsonString, new TypeReference<Map<String, Integer>>() {});
            return new TopicCount(consumerIdString, topicCountMap);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException("error parse consumer json string " + jsonString, e);
        } catch (JsonMappingException e) {
            throw new IllegalArgumentException("error parse consumer json string " + jsonString, e);
        } catch (IOException e) {
            throw new IllegalArgumentException("error parse consumer json string " + jsonString, e);
        }
    }
    
}
