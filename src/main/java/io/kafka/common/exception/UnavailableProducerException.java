package io.kafka.common.exception;
/**
 * @author tf
 * @version 创建时间：2019年1月19日 下午10:20:38
 * @ClassName Producer异常
 */
public class UnavailableProducerException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	
	public UnavailableProducerException() {
        super();
    }

    public UnavailableProducerException(String message) {
        super(message);
    }
}
