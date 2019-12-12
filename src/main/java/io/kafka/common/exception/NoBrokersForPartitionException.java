package io.kafka.common.exception;
/**
 * @author tf
 * @version 创建时间：2019年1月19日 上午11:16:14
 * @ClassName 当请求代理但没有具有该主题的代理时引发
 */
public class NoBrokersForPartitionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NoBrokersForPartitionException() {
        super();
    }

    public NoBrokersForPartitionException(String message) {
        super(message);
    }

}