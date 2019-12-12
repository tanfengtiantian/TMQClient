package io.kafka.network.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * @author tf
 * @version 创建时间：2019年1月21日 下午2:33:53
 * @ClassName 有界发送数据包
 */
public class BoundedByteBufferSend extends AbstractSend {

	final ByteBuffer buffer;

    private ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

    public BoundedByteBufferSend(ByteBuffer buffer) {
        this.buffer = buffer;
        sizeBuffer.putInt(buffer.limit());
        sizeBuffer.rewind();
    }

    public BoundedByteBufferSend(int size) {
        this(ByteBuffer.allocate(size));
    }

    public BoundedByteBufferSend(Request request) {
        this(request.getSizeInBytes() + 2);
        //[type -2bytes]
        buffer.putShort((short)request.getRequestKey().value);
        request.writeTo(buffer);
        buffer.rewind();
    }
    
   
    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = 0;
        // 写入包大小
        if (sizeBuffer.hasRemaining()) written += channel.write(sizeBuffer);
        // 写入包内容
        if (!sizeBuffer.hasRemaining() && buffer.hasRemaining()) written += channel.write(buffer);
        // 写入完成 completed
        if (!buffer.hasRemaining()) { setCompleted();}

        return written;
    }

}
