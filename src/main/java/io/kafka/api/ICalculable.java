package io.kafka.api;
/**
 * @author tf
 * @version 创建时间：2019年1月18日 下午2:20:30
 * @ClassName 标记可计算对象(request/message/data...)
 */
public interface ICalculable {
	/**
     * 获取当前对象的大小（字节）
     * topic + partition + messageSize + message
     * =====================================
     * topic: size(2bytes) + data(utf-8 bytes)
     * partition: int(4bytes)
     * messageSize: int(4bytes)
     * message: bytes
     * @return 
     */
    int getSizeInBytes();

}
