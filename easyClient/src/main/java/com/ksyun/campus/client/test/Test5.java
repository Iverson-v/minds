package com.ksyun.campus.client.test;

import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.domain.ClusterInfo;
import com.ksyun.campus.client.domain.StatInfo;

import java.util.List;

public class Test5 {
    public static void main(String[] args) {
        EFileSystem eFileSystem=new EFileSystem("D:/MyIDE/JavaProjects/minfs_student/dataServerDiskSpace");


        ClusterInfo clusterInfo = eFileSystem.getClusterInfo();
        System.out.println(clusterInfo);
        System.out.println();

        StatInfo fileStats = eFileSystem.getFileStats("/a");
        List<StatInfo> statInfos = eFileSystem.listFileStats("/a");
        statInfos.stream().forEach(System.out::println);

        System.out.println();
        System.out.println(fileStats);





    }
}
