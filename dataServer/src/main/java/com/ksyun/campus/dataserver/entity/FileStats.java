package com.ksyun.campus.dataserver.entity;
public class FileStats {//封装删除文件需要的参数
    public long totalSize;
    public int fileCount;

    public FileStats(long totalSize, int fileCount) {
        this.totalSize = totalSize;
        this.fileCount = fileCount;
    }
}