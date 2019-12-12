package io.kafka.producer;

import io.kafka.api.MultiProducerRequest;
import io.kafka.api.ProducerTTLRequest;
import io.kafka.api.TransactionRequest;
import io.kafka.network.request.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kafka.api.ProducerRequest;
import io.kafka.common.ErrorMapping;
import io.kafka.message.ByteBufferMessageSet;
import io.kafka.network.BlockingChannel;
import io.kafka.network.request.Receive;
import io.kafka.producer.config.SyncProducerConfig;
import io.kafka.utils.KV;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;


/**
 * @author tf
 * @version 创建时间：2019年1月19日 下午2:25:47
 * @ClassName 同步Producer
 */
public class SyncProducer implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SyncProducer.class);

    /////////////////////////////////////////////////////////////////////
    private final SyncProducerConfig config;

    private final BlockingChannel blockingChannel;

    private final Object lock = new Object();

    private volatile boolean shutdown = false;

    private final String host;

    private final int port;

    public SyncProducer(SyncProducerConfig config) {
        super();
        this.config = config;
        this.host = config.getHost();
        this.port = config.getPort();
        //
        this.blockingChannel = new BlockingChannel(host, port, BlockingChannel.DEFAULT_BUFFER_SIZE, config.bufferSize, config.socketTimeoutMs);
    }

    public KV<Receive, ErrorMapping> send(Request request){
        return send(request,null);
    }

    public KV<Receive, ErrorMapping> send(Request request,Callback back) {
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
                disconnect();
                throw new RuntimeException(e);
            } finally {
                if (logger.isDebugEnabled()) {
                    logger.debug("write {} bytes data to {}:{}", written, host, port);
                }
            }
            
        }
    }

    private BlockingChannel connect() {
        if (!blockingChannel.isConnected() && !shutdown) {
            try {
                blockingChannel.connect();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe.getMessage(), ioe);
            } finally {
                if (!blockingChannel.isConnected()) {
                    disconnect();
                }
            }
        }
        return blockingChannel;
    }

    private void disconnect() {
        if (blockingChannel.isConnected()) {
            logger.info("Disconnecting from " + config.getHost() + ":" + config.getPort());
            blockingChannel.disconnect();
        }
    }

    public void close() {
        synchronized (lock) {
            try {
                disconnect();
            } finally {
                shutdown = true;
            }
        }
    }

    /**
     * 事务消息begin
     * @param request
     * @return
     */
    public SendResult sendTransaction(Request request) {
        KV<Receive, ErrorMapping> response = send(request);
        SendResult result = TransactionRequest.deserializeProducer(response.k.buffer(),response.v);
        return result;
    }

    /**
     * 带ack send
     * @param request
     * @param back
     * @return
     */
    public SendResult sendTransactionPutCommand(TransactionRequest request, Callback back) {
        request.messages.verifyMessageSize(config.maxMessageSize);
        KV<Receive, ErrorMapping> response = send(request,back);
        SendResult result =TransactionRequest.deserializeProducer(response.k.buffer(),response.v);
        return result;
    }

    /**
     * 带ack send
     * @param request
     * @return
     */
    public SendResult sendAck(ProducerRequest request) {
        KV<Receive, ErrorMapping> response = send(request);
        SendResult result =ProducerRequest.deserializeProducer(response.k.buffer(),response.v);
        return result;
    }

    public SendResult sendTTL(ProducerTTLRequest request) {
        KV<Receive, ErrorMapping> response = send(request);
        SendResult result =ProducerTTLRequest.deserializeProducer(response.k.buffer(),response.v);
        return result;
    }
    /**
     * 检验消息大小后，构建ProducerRequest对象进行send
     * @param topic
     * @param partition
     * @param messages
     */
	public SendResult send(String topic, int partition, long ttl, ByteBufferMessageSet messages) {
        messages.verifyMessageSize(config.maxMessageSize);
        if(ttl == -1){
            return sendAck(new ProducerRequest(topic, partition, messages));
        }else if(ttl >= 1){
            return sendTTL(new ProducerTTLRequest(topic, partition, ttl, messages));
        }else{
            return null;
        }

	}

	/**
     * 采用随机partition进行发送
     * @param topic
     * @param message
     */
	public void send(String topic, ByteBufferMessageSet message) {
	   send(topic, ProducerRequest. RandomPartition,-1, message);
	}

    public void multiSend(List<ProducerRequest> produces) {
        for (ProducerRequest request : produces) {
            request.messages.verifyMessageSize(config.maxMessageSize);
        }
        send(new MultiProducerRequest(produces));
    }
}
