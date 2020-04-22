package io.kafka.network;

import io.kafka.common.ErrorMapping;
import io.kafka.network.request.BoundedByteBufferReceive;
import io.kafka.network.request.BoundedByteBufferSend;
import io.kafka.network.request.Receive;
import io.kafka.network.request.Request;
import io.kafka.utils.Closer;
import io.kafka.utils.KV;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author tf
 * @version 创建时间：2019年1月19日 上午10:34:22
 * @ClassName 堵塞超时BlockingChannel
 */
public class BlockingChannel {

    public static final int DEFAULT_BUFFER_SIZE = -1;
    private final String host;
    private final int port;
    private final int readBufferSize;
    private final int writeBufferSize;
    private final int readTimeoutMs;

    private boolean connected = false;
    private SocketChannel channel;
    private ReadableByteChannel readChannel;
    private GatheringByteChannel writeChannel;
    private final ReentrantLock lock = new ReentrantLock();

    public BlockingChannel(String host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs) {
        this.host = host;
        this.port = port;
        this.readBufferSize = readBufferSize;
        this.writeBufferSize = writeBufferSize;
        this.readTimeoutMs = readTimeoutMs;
    }

    public void connect() throws IOException {
        lock.lock();
        try {
            if (!connected) {
                channel = SocketChannel.open();
                if (readBufferSize > 0) {
                    channel.socket().setReceiveBufferSize(readBufferSize);
                }
                if (writeBufferSize > 0) {
                    channel.socket().setSendBufferSize(writeBufferSize);
                }
                channel.configureBlocking(true);
                channel.socket().setSoTimeout(readTimeoutMs);
                channel.socket().setKeepAlive(true);
                //屏蔽nagle算法
                channel.socket().setTcpNoDelay(true);
                channel.connect(new InetSocketAddress(host, port));

                writeChannel = channel;
                readChannel = Channels.newChannel(channel.socket().getInputStream());
                connected = true;
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean isConnected() {
        return connected;
    }

    public void disconnect() {
        lock.lock();
        try {
            if (connected || channel != null) {
                Closer.closeQuietly(channel);
                Closer.closeQuietly(channel.socket());
                Closer.closeQuietly(readChannel);
                channel = null;
                readChannel = null;
                writeChannel = null;
                connected = false;
            }

        } finally {
            lock.unlock();
        }
    }

    public int send(Request request) throws IOException {
        if (!isConnected()) {
            throw new ClosedChannelException();
        }
        return new BoundedByteBufferSend(request).writeCompletely(writeChannel);
    }

	public KV<Receive, ErrorMapping> receive() throws IOException {
		BoundedByteBufferReceive response = new BoundedByteBufferReceive();
        response.readCompletely(readChannel);
        return new KV<>(response, ErrorMapping.valueOf(response.buffer().getShort()));
	}
}
