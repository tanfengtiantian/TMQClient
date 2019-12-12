package io.kafka.transaction;

import io.kafka.api.TransactionRequest;
import io.kafka.common.exception.TransactionInProgressException;
import io.kafka.producer.SendResult;
import io.kafka.producer.SyncProducer;
import io.kafka.utils.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事务上下文，同时支持本地事务
 * @author tf
 * @version 创建时间：2019年6月20日
 */
public class TransactionContext {

    private static final Logger logger = LoggerFactory.getLogger(TransactionContext.class);

    private TransactionId transactionId;

    // 默认事务超时时间为10秒
    protected int transactionTimeout = 10;

    //单次请求最大超时毫秒单位
    private final long transactionRequestTimeoutInMills;

    private final LongSequenceGenerator localTxIdGenerator;

    private SyncProducer producer;

    private final long startMs;

    public TransactionContext(int transactionTimeout,long transactionRequestTimeoutInMills){
        this.localTxIdGenerator = new LongSequenceGenerator();
        this.startMs = System.currentTimeMillis();
        this.transactionTimeout = transactionTimeout;
        this.transactionRequestTimeoutInMills = transactionRequestTimeoutInMills;
    }

    public TransactionId getTransactionId() {
        return this.transactionId;
    }

    public void setProducer(SyncProducer producer) {
        this.producer=producer;
    }

    public void begin() {
        if (this.transactionId == null) {
            this.transactionId =
                    new LocalTransactionId(new IdGenerator().generateId(), this.localTxIdGenerator.getNextSequenceId());
            TransactionRequest request = new TransactionRequest(TransactionRequest.BeginTransaction,transactionId.getTransactionKey(),transactionTimeout);
            syncSendLocalTxCommand(request);
        }
    }

    public void rollback(){
        if (this.transactionId != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("RollBack: " + this.transactionId);
            }
            try {
                TransactionRequest request = new TransactionRequest(TransactionRequest.Rollback,transactionId.getTransactionKey());
                this.syncSendLocalTxCommand(request);
            } finally {
                this.logTxTime();
            }

        }
    }

    public void commit() {
        if (this.transactionId != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Commit: " + this.transactionId);
            }
            try {
                TransactionRequest request = new TransactionRequest(TransactionRequest.Commit,transactionId.getTransactionKey());
                this.syncSendLocalTxCommand(request);
            } finally {
                this.logTxTime();
            }
        }
    }

    /**
     * 计算耗时
     */
    private void logTxTime() {
        final long value = System.currentTimeMillis() - this.startMs;
    }

    private void syncSendLocalTxCommand(TransactionRequest request) {
        try {
            SendResult re =producer.sendTransaction(request);

            if(re.isSuccess()){
                if(logger.isDebugEnabled()){
                    logger.debug("Local start xid=["+re.getXid().getTransactionKey()+"] type=["+re.getTransactionType()+"]");
                }
            }else {
                throw new TransactionInProgressException("Local error xid=["+request.getTransactionId()+"]");
            }
        }catch (Exception e){
            throw new TransactionInProgressException(e.getMessage());
        }
    }
}
