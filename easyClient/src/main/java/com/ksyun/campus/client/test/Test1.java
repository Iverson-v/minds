package com.ksyun.campus.client.test;

import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.FSOutputStream;


import java.io.*;

public class Test1 {
    public static void main(String[] args) throws IOException {
        File file=new File("easyClient/src/main/java/com/ksyun/campus/client/test/1.jpg");
//        File file=new File("D:\\MyIDE\\JavaProjects\\TestSDK\\src\\main\\java\\org\\example\\test\\Anaconda3-2021.04-Windows-x86_64.exe");
        FileInputStream fis=new FileInputStream(file);


        EFileSystem eFileSystem=new EFileSystem();
        //调用create之后，在dataserver应该就创建了一个目录。
        FSOutputStream fsOutputStream = eFileSystem.create("/a/cond.exe");

        byte[] buffer=new byte[1024];
        int len = fis.read(buffer);
        while(len!=-1){
            //写到分布式系统中。
            fsOutputStream.write(buffer,0,len);//这个是我自己设计的分布式写方法
            len=fis.read(buffer);
        }
        fis.close();

        fsOutputStream.close();
        System.out.println();


    }
}
