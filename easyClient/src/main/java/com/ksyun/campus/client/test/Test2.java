package com.ksyun.campus.client.test;
import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.util.HttpClientUtils;

public class Test2 {
    public static void main(String[] args) throws Exception {
        EFileSystem eFileSystem=new EFileSystem("D:/MyIDE/JavaProjects/minfs_student/dataServerDiskSpace");
        boolean mkdir = eFileSystem.mkdir("/a/temp");
        System.out.println(mkdir);
//        String url="http://localhost:9003/mkdirMaster?path=/aaa";
//        String jsonStr="[{\"ip\":\"localhost\",\"port\":9002,\"rack\":\"rack1\"," +
//                "\"zone\":\"zone1\",\"fileTotal\":0,\"capacity\":10000,\"useCapacity\":0},{\"ip" +
//                "\":\"localhost\",\"port\":9001,\"rack\":\"rack1\",\"zone\":\"zone1\",\"fileT" +
//                "otal\":0,\"capacity\":10000,\"useCapacity\":0}]";
//
//        String restJson = HttpClientUtils.postParameters(url, jsonStr,"application/json");
//        System.out.println(restJson);
    }
}
