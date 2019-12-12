package io.kafka.api;

import io.kafka.network.request.Request;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author tf
 * @version 创建时间：2019年2月11日 上午11:33:11
 * @ClassName MultiFetchRequest
 */
public class MultiFetchRequest implements Request {

    public final List<FetchRequest> fetches;

    public MultiFetchRequest(List<FetchRequest> fetches) {
        this.fetches = fetches;
    }

    public RequestKeys getRequestKey() {
        return RequestKeys.MULTIFETCH;
    }

    /**
     * @return fetches
     */
    public List<FetchRequest> getFetches() {
        return fetches;
    }

    public int getSizeInBytes() {
        int size = 2;
        for (FetchRequest fetch : fetches) {
            size += fetch.getSizeInBytes();
        }
        return size;
    }

    public void writeTo(ByteBuffer buffer) {
        if (fetches.size() > Short.MAX_VALUE) {//max 32767
            throw new IllegalArgumentException("超过了最大乏值(max 32767)： " + Short.MAX_VALUE + ".");
        }
        buffer.putShort((short) fetches.size());
        for (FetchRequest fetch : fetches) {
            fetch.writeTo(buffer);
        }
    }

    public static MultiFetchRequest readFrom(ByteBuffer buffer) {
        int count = buffer.getShort();
        List<FetchRequest> fetches = new ArrayList<FetchRequest>(count);
        for (int i = 0; i < count; i++) {
            fetches.add(FetchRequest.readFrom(buffer));
        }
        return new MultiFetchRequest(fetches);
    }
}