package io.kafka.rpc.cluster;


import io.kafka.api.RpcRequest;
import io.kafka.common.ErrorMapping;
import io.kafka.common.exception.RpcRuntimeException;
import io.kafka.producer.Callback;
import io.kafka.rpc.Channel;
import io.kafka.rpc.RemotingClient;
import io.kafka.rpc.RpcResponse;

/**
 * 快速失败, 只发起一次调用, 失败立即报错(缺省设置)
 *
 * 通常用于非幂等性的写操作.
 *
 * @author tf
 */
public class FailFastClusterInvoker implements ClusterInvoker {

    final RemotingClient client;

    public FailFastClusterInvoker (RemotingClient client) {
        this.client = client;
    }

    @Override
    public Object invoke(RpcRequest request) {
        Channel channel = client.selectChannel();
        if(channel == null) {
            throw new RpcRuntimeException("Rpc failure:no channels available.");
        }
        RpcResponse response = client.invoke(channel,request,new FailFastClusterInvokerCall());
        if (response == null) {
            throw new RpcRuntimeException("Rpc failure,no response from rpc server.");
        }
        //快速重试
        if(response.getErrorMapping() == ErrorMapping.UnkonwCode){
            return invoke(request);
        }
        if (response.getErrorMapping() == ErrorMapping.NoError) {
            return response.getResult();
        }
        else {
            throw new RpcRuntimeException("Rpc failure:" + response.getErrorMsg());
        }
    }

    class FailFastClusterInvokerCall implements Callback {
        @Override
        public void call(Channel channel, Object object) {
            //移除故障channel
            client.removeChannel(channel);
        }
    }
}
