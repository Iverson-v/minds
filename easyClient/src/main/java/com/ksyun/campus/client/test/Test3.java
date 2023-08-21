package com.ksyun.campus.client.test;

import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.domain.StatInfo;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Test3 {
    public static void main(String[] args) {
//        String path="/a/b/c";
//        String[] split = path.split("/");
//        List<String> collect = Arrays.stream(split).skip(1).collect(Collectors.toList());
//
//        for (String s :
//                collect) {
//            System.out.println(s);
//        }

        EFileSystem eFileSystem=new EFileSystem("D:/MyIDE/JavaProjects/minfs_student/dataServerDiskSpace");
        StatInfo fileStats = eFileSystem.getFileStats("/a/b");
        System.out.println(fileStats.toString());

    }
}
