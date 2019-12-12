package io.kafka.api;

import io.kafka.message.ByteBufferMessageSet;
import io.kafka.common.ErrorMapping;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author tf
 * @version 创建时间：2019年2月11日 下午3:51:36
 * @ClassName MultiFetchResponse
 */
public class MultiFetchResponse implements Iterable<ByteBufferMessageSet> {

    private final List<ByteBufferMessageSet> messageSets;

    /**
     * create multi-response
     * <p>
     * buffer format: <b> size+errorCode(short)+payload+size+errorCode(short)+payload+... </b>
     * <p>
     * size = 2(short)+length(payload)
     * 
     * @param buffer the whole data buffer
     * @param numSets response count
     * @param offsets message offset for each response
     */
    public MultiFetchResponse(ByteBuffer buffer, int numSets, List<Long> offsets) {
        super();
        this.messageSets = new ArrayList<ByteBufferMessageSet>();
        for (int i = 0; i < numSets; i++) {
            int size = buffer.getInt();
            short errorCode = buffer.getShort();
            ByteBuffer copy = buffer.slice();
            int payloadSize = size - 2;
            copy.limit(payloadSize);
            //move position for next reading
            buffer.position(buffer.position() + payloadSize);
            messageSets.add(new ByteBufferMessageSet(copy, offsets.get(i), ErrorMapping.valueOf(errorCode)));
        }
    }

    public Iterator<ByteBufferMessageSet> iterator() {
        return messageSets.iterator();
    }

    public int size() {
        return messageSets.size();
    }
    
    public boolean isEmpty() {
        return size() == 0;
    }
}