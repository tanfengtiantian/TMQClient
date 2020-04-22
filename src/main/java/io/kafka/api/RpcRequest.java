package io.kafka.api;

import io.kafka.common.ErrorMapping;
import io.kafka.common.exception.ErrorMappingException;
import io.kafka.network.request.Request;
import io.kafka.rpc.RpcResponse;
import io.kafka.utils.Utils;
import java.nio.ByteBuffer;

public class RpcRequest implements Request {

    private final String className;

    private final String methodName;

    private Object[] args = null;

    private byte[] argumentsData = null;

    public RpcRequest(String className, String methodName, Object[] args) {
        this.className = className;
        this.methodName = methodName;
        this.args = args;
        argumentsData = Utils.jdkSerializable(args);
    }

    public static RpcResponse deserializeRpcResponse(ByteBuffer buffer, ErrorMapping errorcode) {
        if(errorcode == ErrorMapping.NoError){
            int size = buffer.getInt();
            byte[] bytes = new byte[size];
            buffer.get(bytes);
            return new RpcResponse(errorcode,Utils.jdkDeserialization(bytes));
        }else {
            return new RpcResponse(errorcode,null);
        }
    }

    public String getClassName() {
        return className;
    }

    public String getMethodName() {
        return methodName;
    }

    @Override
    public RequestKeys getRequestKey() {
        return RequestKeys.RPC;
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        Utils.writeShortString(buffer, className);
        Utils.writeShortString(buffer, methodName);
        buffer.putInt(this.args == null ? 0 : this.args.length);
        if (argumentsData != null) {
            buffer.putInt(argumentsData.length);
            buffer.put(argumentsData);
        }
    }

    @Override
    public int getSizeInBytes() {
        return    (Utils.caculateShortString(className)
                + Utils.caculateShortString(methodName)
                + (args != null ? 4 : 0)
                + (argumentsData != null ? 4 : 0)
                + (argumentsData != null ? argumentsData.length : 0));
    }

}
