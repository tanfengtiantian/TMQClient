package io.kafka.producer;

import io.kafka.rpc.Channel;

/**
 * @author tf
 * @version 创建时间：2019年2月28日 下午5:42:39
 * @ClassName Callback
 */
public interface Callback {

    void call(Channel channel, Object object);
}
