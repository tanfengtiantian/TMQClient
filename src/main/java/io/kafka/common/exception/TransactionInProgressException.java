package io.kafka.common.exception;

/**
 * @author tf
 * @version 创建时间：2019年6月20日 上午11:16:14
 * @ClassName 事务引发的异常
 */
public class TransactionInProgressException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TransactionInProgressException() {
        super();
    }

    public TransactionInProgressException(String message) {
        super(message);
    }
}
