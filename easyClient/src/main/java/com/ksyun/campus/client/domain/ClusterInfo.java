package com.ksyun.campus.client.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.checkerframework.checker.units.qual.A;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClusterInfo {
    private MetaServerMsg masterMetaServer;
    private MetaServerMsg slaveMetaServer;
    private List<DataServerMsg> dataServer;


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MetaServerMsg{
        private String host;
        private int port;

    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DataServerMsg{
        private String host;
        private int port;
        private int fileTotal;//文件数量
        private long capacity;//可以写死比如100MB
        private long useCapacity;


    }

    @Override
    public String toString() {
        return "ClusterInfo{" +
                "masterMetaServer=" + masterMetaServer +"\n"+
                ", slaveMetaServer=" + slaveMetaServer +"\n"+
                ", dataServer=" + dataServer +
                '}';
    }
}
