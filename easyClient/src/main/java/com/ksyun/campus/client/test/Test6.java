package com.ksyun.campus.client.test;

import com.ksyun.campus.client.EFileSystem;

import java.io.File;

public class Test6 {
    public static void main(String[] args) {
        EFileSystem eFileSystem=new EFileSystem();
        boolean delete = eFileSystem.delete("/a");
        System.out.println(delete);


    }
}
