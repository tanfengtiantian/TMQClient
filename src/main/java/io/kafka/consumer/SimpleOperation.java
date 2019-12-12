package io.kafka.consumer;

import io.kafka.api.FetchRequest;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.network.BlockingChannel;
import io.kafka.network.request.Receive;
import io.kafka.network.request.Request;
import io.kafka.utils.KV;
import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import io.kafka.common.ErrorMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tf
 * @version 创建时间：2019年1月25日 上午9:29:23
 * @ClassName 消费链接操作
 */
public class SimpleOperation implements Closeable {

	private final Logger logger = LoggerFactory.getLogger(SimpleOperation.class);

    ////////////////////////////////
    private final BlockingChannel blockingChannel;

    private final Object lock = new Object();

    public SimpleOperation(String host, int port) {
        this(host, port, 30 * 1000, 64 * 1024);
    }

    public SimpleOperation(String host, int port, int soTimeout, int bufferSize) {
        blockingChannel = new BlockingChannel(host, port, bufferSize, BlockingChannel.DEFAULT_BUFFER_SIZE, soTimeout);
    }

    private BlockingChannel connect() throws IOException {
        close();
        blockingChannel.connect();
        return blockingChannel;
    }

    private void disconnect() {
        if (blockingChannel.isConnected()) {
            blockingChannel.disconnect();
        }
    }

    public void close() {
        synchronized (lock) {
            blockingChannel.disconnect();
        }
    }

    private void reconnect() throws IOException {
        disconnect();
        connect();
    }


    private void getOrMakeConnection() throws IOException {
        if (!blockingChannel.isConnected()) {
            connect();
        }
    }

    public KV<Receive, ErrorMapping> send(Request request) throws IOException {
        synchronized (lock) {
            getOrMakeConnection();
            try {
                blockingChannel.send(request);
                return blockingChannel.receive();
            } catch (ClosedByInterruptException cbie) {
                logger.info("receive interrupted(接收中断)");
                throw cbie;
            } catch (IOException e) {
                logger.info("Reconnect fetchrequest socket error: {}", e.getMessage());
                //重试一次
                try {
                    reconnect();
                    blockingChannel.send(request);
                    return blockingChannel.receive();
                } catch (SocketTimeoutException ste){
                    logger.error("Reconnect now: {}",ste.getMessage());
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                    return send(request);
                }
                catch (ConnectException ce){
                    logger.error("Reconnect now: {}",ce.getMessage());
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                    return send(request);
                }
                catch (IOException e2) {
                    throw e2;
                }
            }
        }
    }

    public ByteBufferMessageSet fetch(FetchRequest request) throws IOException {
        KV<Receive, ErrorMapping> response = send(request);
        return new ByteBufferMessageSet(response.k.buffer(), request.offset);
    }

}
