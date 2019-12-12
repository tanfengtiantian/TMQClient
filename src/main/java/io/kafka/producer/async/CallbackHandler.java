package io.kafka.producer.async;


import java.util.List;

/**
 * @author tf
 * @since 1.0
 */
public interface CallbackHandler<T> {


    /**
     * 回调以在批处理数据被发送之前对其进行处理
     * by the handle API of the event handler
     *
     * @param data the batched data received by the event handler
     * @return the processed batched data that gets sent by the handle()
     * API of the event handler
     */
    List<QueueItem<T>> beforeSendingData(List<QueueItem<T>> data);


    /**
     * Cleans up and shuts down the callback handler
     */
    void close();
}
