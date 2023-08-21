package com.ksyun.campus.client.test;

import com.ksyun.campus.client.EFileSystem;
import com.ksyun.campus.client.FSInputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class Test8 {
    public static void main(String[] args) throws IOException {
        File file=new File("D:/MyIDE/JavaProjects/minfs_student/easyClient/src/main/java/com/ksyun/campus/client/test/copy.txt");
        FileOutputStream fos = new FileOutputStream(file,false);//是否追加


        EFileSystem eFileSystem=new EFileSystem("D:/MyIDE/JavaProjects/minfs_student/dataServerDiskSpace");
        //找到对应的副本信息
        FSInputStream fis = eFileSystem.open("/a/hello.txt");
        byte[] buffer=new byte[4];

        int read = fis.read();
        System.out.println(read);
        read = fis.read();
        System.out.println(read);
        fis.close();
        int read1 = fis.read(buffer, 1, 3);
        System.out.println(read1);
        for (byte b :
                buffer) {
            System.out.print(b+" ");
        }
        System.out.println();
        System.out.println();


        int len = fis.read(buffer);
        while (len!=-1){

            for (byte b :
                    buffer) {
                System.out.print(b+" ");
            }

            System.out.println("长度："+len);
            System.out.println();


            len = fis.read(buffer);
        }

        //fos.write(buffer);



        fis.close();
        fos.close();





    }
}
