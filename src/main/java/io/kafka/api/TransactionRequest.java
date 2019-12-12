package io.kafka.api;

import io.kafka.cluster.Partition;
import io.kafka.common.ErrorMapping;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.network.request.Request;
import io.kafka.producer.SendResult;
import io.kafka.transaction.TransactionId;
import io.kafka.utils.Utils;

import java.nio.ByteBuffer;


/**
 * @author tf
 * @version 创建时间：2019年6月5日 下午2:23:29
 * @ClassName 服务提供请求类
 * message transaction request
 * <p>
 * TransactionRequest format:
 * <pre>
 * BeginTransaction,Rollback,Prepare,Commit
 * size + type + transactionType + Len(xid) + xid
 * PutCommand
 * size + type + transactionType + Len(xid) + xid + Len(topic) + topic + partition + messageSize + message
 * =====================================
 * size		       : size(4bytes)   32位
 * type		       : type(2bytes)   16位
 * transactionType : type(2bytes)  0-BeginTransaction,1-PutCommand,2-Rollback,3-Prepare,4-Commit
 * Len(xid)        : Len(2bytes)
 * xid             : data(utf-8 bytes)
 * Len(topic)      : Len(2bytes)
 * topic	       : data(utf-8 bytes)
 * partition       : int(4bytes)
 * messageSize:    : int(4bytes)
 * message         : bytes
 */
public class TransactionRequest implements Request {

    public static final int BeginTransaction=0;
    public static final int PutCommand=1;
    public static final int Rollback=2;
    public static final int Prepare=3;
    public static final int Commit=4;
    private String topic = null;
    private Integer partition = null;
    private int transactionType;
    private int transactionTimeout = -1;
    private final String xid;
    /**
     * request messages
     */
    public ByteBufferMessageSet messages;

    @Override
    public RequestKeys getRequestKey() { return RequestKeys.TRANSACTION; }

    public TransactionRequest(int transactionType, String xid) {
        this.transactionType = transactionType;
        this.xid = xid;
    }

    public TransactionRequest(int transactionType, String xid, int transactionTimeout) {
        this.transactionType = transactionType;
        this.xid = xid;
        this.transactionTimeout = transactionTimeout;
    }

    public TransactionRequest(int transactionType, String xid, String topic, int partition, ByteBufferMessageSet messages) {
        this.transactionType=transactionType;
        this.xid=xid;
        this.topic=topic;
        this.partition=partition;
        this.messages=messages;
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        buffer.putShort((short) transactionType);
        Utils.writeShortString(buffer, xid);
        if(transactionTimeout >= 0){
            buffer.putInt(transactionTimeout);
        }
        if(topic != null){
            Utils.writeShortString(buffer, topic);
        }
        if(partition != null){
            buffer.putInt(partition);
        }
        if(messages != null){
            final ByteBuffer sourceBuffer = messages.serialized();
            buffer.putInt(sourceBuffer.limit());
            buffer.put(sourceBuffer);
            sourceBuffer.rewind();
        }
    }

    @Override
    public int getSizeInBytes() {
        return  (int)(2 // transactionType : type(2bytes)
                + Utils.caculateShortString(xid)//Len(xid) : Len(2bytes)  + xid:bytes
                + (transactionTimeout < 0 ? 0 : 4)
                + (topic == null ? 0 : Utils.caculateShortString(topic))  //Len(topic) : Len(2bytes)  + topic : bytes
                + (partition == null ? 0 : 4) //partition : 4bytes
                + (messages == null ? 0 : 4 + messages.getSizeInBytes()));//messageSize:: int(4bytes)  +  message : bytes
    }


    public ByteBufferMessageSet getMessages() {
        return messages;
    }


    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    public static SendResult deserializeProducer(ByteBuffer buffer, ErrorMapping errorcode) {
        int transactionType = buffer.getShort();
        int brokerId = buffer.getInt();
        String xid = Utils.readShortString(buffer);
        return new SendResult(TransactionId.valueOf(xid),transactionType,new Partition(brokerId,-1),errorcode);
    }

    public String getTransactionId() { return xid; }
}
