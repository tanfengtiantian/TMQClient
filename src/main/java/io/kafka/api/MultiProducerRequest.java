package io.kafka.api;

import io.kafka.network.request.Request;

import java.nio.ByteBuffer;
import java.util.List;

public class MultiProducerRequest implements Request {

    public final List<ProducerRequest> produces;

    public MultiProducerRequest(List<ProducerRequest> produces) {
        this.produces = produces;
    }

    @Override
    public RequestKeys getRequestKey() {
        return RequestKeys.MULTIPRODUCE;
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        if (produces.size() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Number of requests in MultiFetchRequest exceeds " + Short.MAX_VALUE + ".");
        }
        buffer.putShort((short) produces.size());
        for (ProducerRequest produce : produces) {
            produce.writeTo(buffer);
        }
    }

    @Override
    public int getSizeInBytes() {
        int size = 2;
        for (ProducerRequest produce : produces) {
            size += produce.getSizeInBytes();
        }
        return size;
    }
}
