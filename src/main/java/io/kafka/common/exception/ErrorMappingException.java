package io.kafka.common.exception;

import io.kafka.common.ErrorMapping;

/**
 * @author tf
 * @version 创建时间：2019年1月19日 上午11:17:48
 * @ClassName 类名称
 * @Description 类描述
 */
public class ErrorMappingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ErrorMappingException() {
    }

    public ErrorMappingException(String message) {
        super(message);
    }

    public ErrorMappingException(Throwable cause) {
        super(cause);
    }

    public ErrorMappingException(String message, Throwable cause) {
        super(message, cause);
    }

    public ErrorMapping getErrorMapping() {
        return ErrorMapping.UnkonwCode;
    }
}