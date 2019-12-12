package io.kafka.transaction;

import io.kafka.cluster.Partition;
import io.kafka.producer.SyncProducer;


/**
 * @author tf
 * @version 创建时间：2019年6月20日
 */
public interface ITransactionProducer {

    /**
     * 开启一个事务并关联到当前线程，在事务内发送的消息将作为一个单元提交给服务器，要么全部发送成功，要么全部失败
     *
     * @throws Exception
     *             如果已经处于事务中，则抛出TransactionInProgressException异常
     */
    void beginTransaction() throws Exception;

    /**
     * 设置事务超时时间，从事务开始计时，如果超过设定时间还没有提交或者回滚，则服务端将无条件回滚该事务。
     *
     * @param seconds
     *            事务超时时间，单位：秒
     * @throws Exception
     * @see #beginTransaction()
     * @see #rollback()
     * @see #commit()
     */
     void setTransactionTimeout(int seconds) throws Exception;


    /**
     * 返回当前设置的事务超时时间，默认为0,表示永不超时
     *
     * @return 事务超时时间，单位：秒
     * @throws Exception
     */
     int getTransactionTimeout() throws Exception;


    /**
     * 回滚当前事务内所发送的任何消息，此方法仅能在beginTransaction之后调用
     *
     * @throws Exception
     * @see #beginTransaction()
     */
     void rollback() throws Exception;

    /**
     * 提交当前事务，将事务内发送的消息持久化，此方法仅能在beginTransaction之后调用
     *
     * @see #beginTransaction()
     * @throws Exception
     */
     void commit() throws Exception;


    /**
     * 返回本生产者的分区选择器
     *
     * @return
     */
     Partition getPartitionSelector();

    /**
     * 判断是否处于事务中
     *
     * @return
     */
    boolean isInTransaction();

    /**
     * 获取事务上下文
     *
     * @return
     */
    TransactionContext getTx();
    /**
     * 返回上一次发送消息的broker
     *
     * @return
     */
    Partition getLastSentInfo();

    /**
     * 记录上一次投递信息
     *
     * @return
     */
    void setLastSentInfo(Partition part);



}
