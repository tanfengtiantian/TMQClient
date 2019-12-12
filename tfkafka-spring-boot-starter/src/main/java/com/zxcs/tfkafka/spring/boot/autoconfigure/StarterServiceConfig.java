package com.zxcs.tfkafka.spring.boot.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tfkafka 配置
 * <p>
 * Created by zfh on 2019/08/02
 */
@ConfigurationProperties("tfkafka.service")
public class StarterServiceConfig {

    private String zkConnect;

    private String groupid;

    private String brokerList;

    public String getZkConnect() {
        return zkConnect;
    }

    public void setZkConnect(String zkConnect) {
        this.zkConnect = zkConnect;
    }

    public String getGroupid() {
        return groupid;
    }

    public void setGroupid(String groupid) {
        this.groupid = groupid;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    @Override
    public String toString() {
        return "StarterServiceConfig{" +
                "zkConnect='" + zkConnect + '\'' +
                ", groupid='" + groupid + '\'' +
                ", brokerList='" + brokerList + '\'' +
                '}';
    }
}
