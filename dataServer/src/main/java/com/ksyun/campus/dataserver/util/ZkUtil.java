package com.ksyun.campus.dataserver.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class ZkUtil {

    //主ms在zookeeper上地址约定
    private static final String masterMetaServer="/metaServers/localhost:8000";
    //副ms在zookeeper上地址约定
    private static final String secondaryMetaServer="/metaServers/localhost:8001";

    private static String metaServer;

    @Autowired
    private Environment environment;
    private CuratorFramework curatorFramework=null;
    private static  String connectString;
    private static final int sessionTimeoutMs=10000;//会话超时时间，单位毫秒，默认为60000ms。
    private static final int baseSleepTimeMs=1000;
    private static final int maxRetries=10;//zk连接重试次数

    @PostConstruct
    public void postCons() throws Exception {
        // todo 初始化，与zk建立连接，注册监听路径，当配置有变化随时更新



        //todo 先连接zk
        connectString=environment.getProperty("zookeeper.addr");
        //1.创建重试策略   第一个参数会话是睡眠多久重试一次。   第二个是重试次数
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(baseSleepTimeMs,maxRetries);

        //2.创建对象  sessionTimeoutMs，会话超时时间，单位毫秒，默认为60000ms。
        curatorFramework= CuratorFrameworkFactory.builder()
                .retryPolicy(retryPolicy)
                .connectString(connectString)
                .sessionTimeoutMs(sessionTimeoutMs)
                .build();

        //3.开始连接
        curatorFramework.start();



    }

    public CuratorFramework getCuratorFramework() {
        return curatorFramework;
    }

    public   String getMetaServerData(){
        //1.创建重试策略   第一个参数会话是睡眠多久重试一次。   第二个是重试次数
        curatorFramework = getCuratorFramework();


        //4.监听一个metaserver
        metaServer=null;
        //首先去主metaserver访问，如果主metaserver挂了，才去副节点
        Stat stat = null;
        try {
            stat = curatorFramework.checkExists().forPath(masterMetaServer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if(stat==null){
            Stat secondaryStat = null;
            try {
                secondaryStat = curatorFramework.checkExists().forPath(secondaryMetaServer);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (secondaryStat!=null){
                metaServer=secondaryMetaServer;
            }
        }else {
            metaServer=masterMetaServer;
        }


        try {
            if(metaServer!=null){
                //获取ms，从两台中选一台。先选主，再选从
                byte[] bytes = curatorFramework.getData().forPath(metaServer);
                return new String(bytes);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
