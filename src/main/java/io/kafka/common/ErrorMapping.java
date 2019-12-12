package io.kafka.common;

import io.kafka.common.exception.InvalidMessageException;
import io.kafka.common.exception.InvalidMessageSizeException;

import java.nio.ByteBuffer;

/**
 * @author tf
 * @version 创建时间：2019年1月24日 上午10:12:17
 * @ClassName 错误代码和异常的双向映射
 */
public enum ErrorMapping {

    UnkonwCode(-1), //
    NoError(0), //
    OffsetOutOfRangeCode(1), //
    InvalidMessageCode(2), //
    WrongPartitionCode(3), //
    InvalidFetchSizeCode(4);

    public final short code;

    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    ErrorMapping(int code) {
        this.code = (short) code;
    }

    public static ErrorMapping valueOf(Exception e) {
        Class<?> clazz = e.getClass();
        if (clazz == InvalidMessageException.class) {
            return InvalidMessageCode;
        }
        if (clazz == InvalidMessageSizeException.class) {
            return InvalidFetchSizeCode;
        }
        return UnkonwCode;
    }

    public static ErrorMapping valueOf(short code) {
        switch (code) {
            case 0:
                return NoError;
            case 1:
                return OffsetOutOfRangeCode;
            case 2:
                return InvalidMessageCode;
            case 3:
                return WrongPartitionCode;
            case 4:
                return InvalidFetchSizeCode;
            default:
                return UnkonwCode;
        }
    }
}
