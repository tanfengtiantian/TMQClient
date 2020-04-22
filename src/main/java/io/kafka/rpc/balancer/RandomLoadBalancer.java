package io.kafka.rpc.balancer;

import io.kafka.rpc.Channel;

import java.util.concurrent.ThreadLocalRandom;

public class RandomLoadBalancer implements LoadBalancer {

    @Override
    public Channel select(Channel[] channels) {
        int length = channels.length;

        if (length == 0) {
            return null;
        }

        if (length == 1) {
            return channels[0];
        }

        ThreadLocalRandom random = ThreadLocalRandom.current();

        return channels[random.nextInt(length)];
    }
}
