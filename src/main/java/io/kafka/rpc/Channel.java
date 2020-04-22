package io.kafka.rpc;

import io.kafka.api.RpcRequest;
import io.kafka.cluster.Broker;
import io.kafka.common.ErrorMapping;
import io.kafka.network.BlockingChannel;
import io.kafka.network.request.Receive;
import io.kafka.network.request.Request;
import io.kafka.producer.Callback;
import io.kafka.utils.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;


public class Channel implements Closeable {

    private final static Logger logger = LoggerFactory.getLogger(Channel.class);

    /////////////////////////////////////////////////////////////////////
    private final BlockingChannel blockingChannel;

    private final Object lock = new Object();

    private volatile boolean shutdown = false;

    private final String host;

    private final int port;

    private final int weight;

    public Channel(Broker broker, RpcProducerConfig config) {
        this.host = broker.host;
        this.port = broker.port;
        this.weight = broker.weight;
        //
        this.blockingChannel = new BlockingChannel(host, port, BlockingChannel.DEFAULT_BUFFER_SIZE, config.bufferSize, config.socketTimeoutMs);
    }

    public RpcResponse invoke(Request request, Callback back) {
        KV<Receive, ErrorMapping> response = send(request,back);
        RpcResponse rpcResponse = RpcRequest.deserializeRpcResponse(
                 response.k ==null ? ByteBuffer.allocate(0) : response.k.buffer()
                ,response.v);
        return rpcResponse;
    }

    public KV<Receive, ErrorMapping> send(Request request, Callback back) {
        synchronized (lock) {
            long startTime = System.nanoTime();
            int written = -1;
            try {
                written = connect().send(request);
                final long endTime = System.nanoTime();
                if (logger.isDebugEnabled()) {
                    logger.debug("耗时 : {} ", endTime - startTime);
                }
                return connect().receive();
            } catch (IOException e) {
                // 无法判断写得是否成功。断开连接并重新抛出异常，让客户机处理重试
                logger.error(e.getMessage());
                if(back != null) {
                    back.call(this, request);
                }
                disconnect();
                return new KV<>(null, ErrorMapping.UnkonwCode);
            } finally {
                if (logger.isDebugEnabled()) {
                    logger.debug("write {} bytes data to {}:{}", written, host, port);
                }
            }
        }
    }


    private BlockingChannel connect() throws IOException {
        if (!blockingChannel.isConnected() && !shutdown) {
            try {
                blockingChannel.connect();
            } catch (IOException ioe) {
                throw ioe;
            } finally {
                if (!blockingChannel.isConnected()) {
                    disconnect();
                }
            }
        }
        return blockingChannel;
    }

    @Override
    public void close() {
        synchronized (lock) {
            try {
                disconnect();
            } finally {
                shutdown = true;
            }
        }

    }

    private void disconnect() {
        if (blockingChannel.isConnected()) {
            logger.info("Disconnecting from " + host + ":" + port);
            blockingChannel.disconnect();
        }
    }
}
