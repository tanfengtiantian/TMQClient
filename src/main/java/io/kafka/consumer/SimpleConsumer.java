package io.kafka.consumer;

import io.kafka.api.FetchRequest;
import io.kafka.api.MultiFetchRequest;
import io.kafka.api.MultiFetchResponse;
import io.kafka.api.OffsetRequest;
import io.kafka.common.ErrorMapping;
import io.kafka.network.request.Receive;
import io.kafka.utils.KV;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author tf
 * @version 创建时间：2019年1月25日 上午10:04:02
 * @ClassName SimpleConsumer
 */
public class SimpleConsumer extends SimpleOperation implements IConsumer {

	public SimpleConsumer(String host, int port) {
		super(host, port);
	}
	
	public SimpleConsumer(String host, int port, int soTimeout, int bufferSize) {
        super(host, port, soTimeout, bufferSize);
    }
	
	public long[] getOffsetsBefore(String topic, int partition, long time, int maxNumOffsets) throws IOException {
        KV<Receive, ErrorMapping> response = send(new OffsetRequest(topic, partition, time, maxNumOffsets));
        return OffsetRequest.deserializeOffsetArray(response.k.buffer());
    }

	public MultiFetchResponse multifetch(List<FetchRequest> fetches) throws IOException {
        KV<Receive, ErrorMapping> response = send(new MultiFetchRequest(fetches));
        List<Long> offsets = new ArrayList<Long>();
        for (FetchRequest fetch : fetches) {
            offsets.add(fetch.offset);
        }
        return new MultiFetchResponse(response.k.buffer(), fetches.size(), offsets);
    }
	
	

}
