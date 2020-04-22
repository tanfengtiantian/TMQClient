package io.kafka.rpc;

import io.kafka.common.ErrorMapping;

import java.net.InetSocketAddress;

public class RpcResponse {

    private InetSocketAddress responseHost;
    private final ErrorMapping errorMessage;
    private Object result;

    public RpcResponse(final ErrorMapping errorMessage, final Object result) {
        this.errorMessage = errorMessage;
        this.result = result;
    }

    public Object getResult() {
        return this.result;
    }
    private String errorMsg;


    public String getErrorMsg() {
        return this.errorMsg;
    }


    public void setErrorMsg(final String errorMsg) {
        this.errorMsg = errorMsg;

    }


    public InetSocketAddress getResponseHost() {
        return this.responseHost;
    }


    public ErrorMapping getErrorMapping() {
        return this.errorMessage;
    }


    public boolean isBoolean() {
        return false;
    }


    public void setResponseHost(final InetSocketAddress address) {
        this.responseHost = address;

    }
}
