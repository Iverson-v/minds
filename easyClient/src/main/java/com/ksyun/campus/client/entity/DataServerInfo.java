package com.ksyun.campus.client.entity;


import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class DataServerInfo {

    private String host;
    private int port;

//    private String rack;
//    private String zone;
    private int fileTotal;//文件数量
    private long capacity=100*1024*1024;

    private long useCapacity;



    public DataServerInfo(String ip, int port, long capacity, String rack, String zone) {
        this.host = ip;
        this.port = port;
        this.capacity = capacity;
//        this.rack = rack;
//        this.zone = zone;
    }
}

