package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.ReplicaData;
import com.ksyun.campus.client.entity.ByteAndIntVo;
import com.ksyun.campus.client.util.HttpClientUtils;
import com.ksyun.campus.client.util.jaksonutils.JacksonUtil;

import java.io.IOException;
import java.io.InputStream;

public class FSInputStream extends InputStream {
    private String path;
    private String fileSystem;
    private ReplicaData replicaData;

    public void setPath(String path) {
        this.path = path;
    }

    public void setFileSystem(String fileSystem) {
        this.fileSystem = fileSystem;
    }

    public void setReplicaData(ReplicaData replicaData) {
        this.replicaData = replicaData;
    }

    //一个字节一个字节写
    @Override
    public int read() throws IOException {
        String dsNode = replicaData.getDsNode();// ip:port
        String url="http://"+dsNode+"/read?path="+path+"&length=1"+"&offset=0";
        String json;
        try {json = HttpClientUtils.getAndSetHeader(url, "fileSystem", fileSystem);
        } catch (Exception e) {throw new RuntimeException(e);}
        ByteAndIntVo byteAndIntVo = JacksonUtil.toBean(json, ByteAndIntVo.class);
        byte[] bytes = byteAndIntVo.getBytes();

        //直接返回这个字节
        return bytes[0];
    }

    @Override
    public int read(byte[] b) throws IOException {//把读到的字节放到缓冲区b中
        int length=b.length;
        String dsNode = replicaData.getDsNode();// ip:port
        String url="http://"+dsNode+"/read?path="+path+"&length="+length+"&offset=0";
        String json;
        try {json = HttpClientUtils.getAndSetHeader(url, "fileSystem", fileSystem);
        } catch (Exception e) {throw new RuntimeException(e);}
        ByteAndIntVo byteAndIntVo = JacksonUtil.toBean(json, ByteAndIntVo.class);
        byte[] bytes = byteAndIntVo.getBytes();


        int point = byteAndIntVo.getPoint();
        if (point==-1){//发现是-1表示读完了，直接返回-1
            return point;
        }
        System.arraycopy(bytes, 0, b, 0, point); // 更高效的数组复制

        return point;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {


        String dsNode = replicaData.getDsNode();// ip:port
        String url="http://"+dsNode+"/read?path="+path+"&length="+len+"&offset=0";
        String json;
        try {json = HttpClientUtils.getAndSetHeader(url, "fileSystem", fileSystem);
        } catch (Exception e) {throw new RuntimeException(e);}
        ByteAndIntVo byteAndIntVo = JacksonUtil.toBean(json, ByteAndIntVo.class);
        byte[] bytes = byteAndIntVo.getBytes();


        int point = byteAndIntVo.getPoint();
        System.arraycopy(bytes, 0, b, off, point); // 更高效的数组复制

        return point;
    }

    @Override
    public void close() throws IOException {//关闭ds中的流，防止内存泄漏。
        String dsNode = replicaData.getDsNode();// ip:port
        String url="http://"+dsNode+"/closeReadStream?path="+path;
        try {
            HttpClientUtils.getAndSetHeader(url, "fileSystem", fileSystem);

        } catch (Exception e) {throw new RuntimeException(e);}
    }
}
