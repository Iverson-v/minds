package com.ksyun.campus.dataserver.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class ReplicaData {
    public String id;  //副本对应id
    public String dsNode;//格式为ip:port
    public String path;  //该文件存在副本的路径

    @Override
    public String toString() {
        return "ReplicaData{" +
                "id='" + id + '\'' +
                ", dsNode='" + dsNode + '\'' +
                ", path='" + path + '\'' +
                '}';
    }

}
