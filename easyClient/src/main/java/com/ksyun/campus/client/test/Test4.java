package com.ksyun.campus.client.test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Test4 {
    public static void main(String[] args) {
//        File file=new File("D:\\MyIDE\\JavaProjects\\minfs_student\\dataServerDiskSpace\\ds3\\a\\b\\c");
//        boolean mkdirs = file.mkdirs();
//        System.out.println(mkdirs);



        String a="/";
        String[] split = a.split("/");
        System.out.println(split.length);
        List<String> collect = Arrays.stream(split).skip(1).limit(Math.max(0,split.length-2)).collect(Collectors.toList());
        System.out.println(collect);
    }
}
