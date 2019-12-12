package io.kafka.consumer;

import static io.kafka.utils.Utils.getInt;
import static io.kafka.utils.Utils.getString;
import io.kafka.utils.Utils;

import java.util.Properties;

/**
 * @author tf
 * @version 创建时间：2019年1月31日 下午2:17:10
 * @ClassName ConsumerConfig
 */
public class ConsumerConfig {
	
	public static final String SMALLES_TIME_STRING = "smallest";

    public static final String LARGEST_TIME_STRING = "largest";
    
    /**
     * reading the latest offset
     */
    public static final long LATES_TTIME = -1L;

    /**
     * reading the earilest offset
     */
    public static final long EARLIES_TTIME = -2L;
    
	protected final Properties props;
	
	private String groupId;

    private String consumerId;

    private int socketTimeoutMs;

    private int socketBufferSize;

    private int fetchSize;

    private long fetchBackoffMs;

    private long maxFetchBackoffMs;

    private boolean autoCommit;

    private int autoCommitIntervalMs;

    private int maxQueuedChunks;

    private int maxRebalanceRetries;

    private String autoOffsetReset;

    private int consumerTimeoutMs;


	public ConsumerConfig(Properties props) {
		this.props = props;
        this.groupId = Utils.getString(props, "groupid");
        this.consumerId = Utils.getString(props, "consumerid", null);
        this.socketTimeoutMs = get("socket.timeout.ms", 30 * 1000);
        this.socketBufferSize = get("socket.buffersize", 64 * 1024);//64KB
        this.fetchSize = get("fetch.size", 1024 * 1024);//1MB
        this.fetchBackoffMs = get("fetcher.backoff.ms", 1000);
        this.maxFetchBackoffMs = get("fetcher.backoff.ms.max", (int) fetchBackoffMs * 10);
        this.autoCommit = Utils.getBoolean(props, "autocommit.enable", true);
        this.autoCommitIntervalMs = get("autocommit.interval.ms", 1000);//1 seconds
        this.maxQueuedChunks = get("queuedchunks.max", 10);
        this.maxRebalanceRetries = get("rebalance.retries.max", 4);
        this.autoOffsetReset = get("autooffset.reset", ConsumerConfig.SMALLES_TIME_STRING);
        this.consumerTimeoutMs = get("consumer.timeout.ms", -1);

    }
	
	public String getZkConnect() {
        return getString(props, "zk.connect", null);
    }
	
	public int getZkSessionTimeoutMs() {
		return getInt(props, "zk.sessiontimeout.ms", 6000);
	}
	
	public int getZkConnectionTimeoutMs() {
		return getInt(props, "zk.connectiontimeout.ms", 6000);
	}
	
    public String getGroupId() {
        return groupId;
    }

    /**
     * consumerId:如果未设置，则自动生成
     * @return consumerId
     */
    public String getConsumerId() {
        return consumerId;
    }

    /** 网络超时时间
     * @return socket timeout in milliseconds
     */
    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    /** socket 接收 buffer 最大值
     * @return buffer size
     */
    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    /** 每次获取的字节数
     * @return socket message size
     */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * 为了避免重复地轮询没有新数据的代理节点，每次从代理获取空集时，都会进行回退
     * @return 
     */
    public long getFetchBackoffMs() {
        return fetchBackoffMs;
    }

    /**
     * 自动提交offset
     * consumer
     * @return check
     */
    public boolean isAutoCommit() {
        return autoCommit;
    }

    /**
     * 自动提交offset毫秒
     * @return 
     */
    public int getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    /** 消费的最大消息缓冲数量
     * @return max number messages
     */
    public int getMaxQueuedChunks() {
        return maxQueuedChunks;
    }

    /** 最大重试次数
     * @return max number 
     */
    public int getMaxRebalanceRetries() {
        return maxRebalanceRetries;
    }

    /**
     * 偏移量超出范围修复方式
     * 
     * <pre>
     *     smallest : 自动将偏移重置为最小偏移
     *     largest : 自动将偏移重置为最大偏移
     * </pre>
     * @return
     */
    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    /**
     * 如果没有可用的消息，则向使用者抛出超时异常
     * 
     * @return timeout message
     */
    public int getConsumerTimeoutMs() {
        return consumerTimeoutMs;
    }

    /**
     * 获取消息时间
     * @return backoff time
     */
    public long getMaxFetchBackoffMs() {
        return maxFetchBackoffMs;
    }
    
    protected int get(String name,int defaultValue) {
        return getInt(props,name,defaultValue);
    }
    
    protected String get(String name,String defaultValue) {
        return getString(props,name,defaultValue);
    }
    /**
     * 负载分配失败，重试间隔时间
     * @return
     */
	public long getRebalanceBackoffMs() {
		return 1000;
	}
}
