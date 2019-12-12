package io.kafka.common.exception;

import io.kafka.common.ErrorMapping;

/**
 * @author tf
 * @version 创建时间：2019年1月19日 上午11:17:00
 * @ClassName 指示分区ID不在0和numpartitions-1之间
 */
public class InvalidPartitionException extends ErrorMappingException {

    private static final long serialVersionUID = 1L;

    public InvalidPartitionException() {
        super();
    }

    public InvalidPartitionException(String message) {
        super(message);
    }

    @Override
    public ErrorMapping getErrorMapping() {
        return ErrorMapping.WrongPartitionCode;
    }
}
