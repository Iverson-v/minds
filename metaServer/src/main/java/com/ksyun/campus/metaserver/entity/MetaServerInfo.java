package com.ksyun.campus.metaserver.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MetaServerInfo {
    private String host;
    private int port;


    public MetaServerInfo(String ip, int port) {
        this.host = ip;
        this.port = port;
    }
}
