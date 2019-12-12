package io.kafka.producer;

import io.kafka.cluster.Partition;
import io.kafka.common.ErrorMapping;
import io.kafka.transaction.TransactionId;

/**
 * @author tf
 * @version 创建时间：2019年3月2日 上午9:37:21
 * @ClassName 消息发送结果对象
 */
public class SendResult {
	private String topic;
	private Partition partition;
    private ErrorMapping errorMessage;
    private long offset;
    private TransactionId xid;
    private int transactionType = -1;

    public SendResult(String topic, Partition partition, long offset, ErrorMapping errorMessage) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.errorMessage = errorMessage;
    }

    public SendResult(TransactionId xid, int transactionType,Partition partition, ErrorMapping errorMessage) {
        this.xid = xid;
        this.partition = partition;
        this.transactionType=transactionType;
        this.errorMessage = errorMessage;
    }


    
    /**
     * 当消息发送成功后，消息在服务端写入的offset，如果发送失败，返回-1
     * 
     * @return
     */
    public long getOffset() {
        return this.offset;
    }

    public String getTopic() {
		return topic;
	}
    /**
     * 消息是否发送成功
     * 
     * @return true为成功
     */
    public boolean isSuccess() {
        return errorMessage==ErrorMapping.NoError ? true:false;
    }


    /**
     * 消息发送所到达的分区
     * 
     * @return 消息发送所到达的分区，如果发送失败则为null
     */
    public Partition getPartition() {
        return this.partition;
    }


    /**
     * 消息发送结果的附带信息，如果发送失败可能包含错误信息
     * 
     * @return 消息发送结果的附带信息，如果发送失败可能包含错误信息
     */
    public ErrorMapping getErrorMessage() {
        return this.errorMessage;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.errorMessage == null ? 0 : this.errorMessage.hashCode());
        result = prime * result + (int) (this.offset ^ this.offset >>> 32);
        result = prime * result + (this.partition == null ? 0 : this.partition.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        SendResult other = (SendResult) obj;
        if (this.errorMessage == null) {
            if (other.errorMessage != null) {
                return false;
            }
        }
        else if (!this.errorMessage.equals(other.errorMessage)) {
            return false;
        }
        if (this.offset != other.offset) {
            return false;
        }
        if (this.partition == null) {
            if (other.partition != null) {
                return false;
            }
        }
        else if (!this.partition.equals(other.partition)) {
            return false;
        }
        return true;
    }

    public TransactionId getXid() {
        return xid;
    }

    public void setXid(TransactionId xid) {
        this.xid = xid;
    }

    public Integer getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(Integer transactionType) {
        this.transactionType = transactionType;
    }
}
