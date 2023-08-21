package com.ksyun.campus.client.test;

import com.ksyun.campus.client.domain.FileType;
import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.util.ZkUtil;
import com.ksyun.campus.client.util.jaksonutils.JacksonUtil;
import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Test10 {


    public static void main(String[] args) {
        CuratorFramework cf = ZkUtil.getCuratorFramework();
        List<StatInfo> files = scanAllFiles(cf,"/stat"); // 全量扫描文件列表
        files.forEach(System.out::println); // 打印获取的值
    }

    private static List<StatInfo> scanAllFiles(CuratorFramework cf,String path) {
        // 实现全量扫描文件列表的逻辑
        List<String> list=new ArrayList<>();
        try {
            recursiveFetch(cf,path,list);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return list.stream().
                map(s -> JacksonUtil.toBean(s,StatInfo.class)).
                filter(statInfo -> statInfo.getType().equals(FileType.File)).
                collect(Collectors.toList());
    }
    public static void recursiveFetch(CuratorFramework client, String path, List<String> values) throws Exception {
        if (client.checkExists().forPath(path) != null) {
            List<String> children = client.getChildren().forPath(path);
            for (String child : children) {
                String childPath = path + (path.endsWith("/") ? "" : "/") + child;
                byte[] data = client.getData().forPath(childPath);
                if (data != null) {
                    values.add(new String(data));
                }
                recursiveFetch(client, childPath, values);
            }
        }
    }
}
