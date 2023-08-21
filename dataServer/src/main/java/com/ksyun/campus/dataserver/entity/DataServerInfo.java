package com.ksyun.campus.dataserver.entity;


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
    private long capacity=1024*1024*1024;

    private long useCapacity;



    public DataServerInfo(String host, int port, long capacity) {
        this.host = host;
        this.port = port;
        this.capacity = capacity;
//        this.rack = rack;
//        this.zone = zone;
    }
}

