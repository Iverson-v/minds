package com.ksyun.campus.client.test;

import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.FSInputStream;
import com.ksyun.campus.client.FSOutputStream;

import java.io.*;

public class Test7 {
    public static void main(String[] args) throws IOException {
        File file=new File("D:/MyIDE/JavaProjects/minfs_student/easyClient/src/main/java/com/ksyun/campus/client/test/2.jpg");
        FileOutputStream fos = new FileOutputStream(file,false);//是否追加


        EFileSystem eFileSystem=new EFileSystem("D:/MyIDE/JavaProjects/minfs_student/dataServerDiskSpace");
        //找到对应的副本信息
        FSInputStream fis = eFileSystem.open("/a/first.jpg");
        byte[] buffer=new byte[1024];

        int len = fis.read(buffer);
        while(len!=-1){
            fos.write(buffer,0,len);
            len=fis.read(buffer);
        }
        fis.close();
        fos.close();





    }
}
