package com.ksyun.campus.client.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import javax.annotation.PostConstruct;


public class ZkUtil {
    //主ms在zookeeper上地址约定
    private static final String masterMetaServer="/metaServers/localhost:8000";
    //副ms在zookeeper上地址约定
    private static final String secondaryMetaServer="/metaServers/localhost:8001";

    private static String metaServer;
    private  static CuratorFramework curatorFramework=null;
    private static final String connectSTring="10.0.0.201:2181";
//    private static final String connectSTring="localhost:2181";
    private static final int sessionTimeoutMs=10000;//会话超时时间，单位毫秒，默认为60000ms。
    private static final int baseSleepTimeMs=1000;
    private static final int maxRetries=10;//zk连接重试次数

    //单例模式

    public static CuratorFramework getCuratorFramework(){
        if(curatorFramework==null){
            synchronized (CuratorFramework.class){
                if(curatorFramework==null){
                    //1.创建重试策略   第一个参数会话是睡眠多久重试一次。   第二个是重试次数
                    RetryPolicy retryPolicy=new ExponentialBackoffRetry(baseSleepTimeMs,maxRetries);

                    //2.创建对象  sessionTimeoutMs，会话超时时间，单位毫秒，默认为60000ms。
                    curatorFramework= CuratorFrameworkFactory.builder()
                            .retryPolicy(retryPolicy)
                            .connectString(connectSTring)
                            .sessionTimeoutMs(sessionTimeoutMs)
                            .build();
                    //启动
                    curatorFramework.start();

                    return curatorFramework;
                }
                return curatorFramework;
            }
        }else
            return curatorFramework;
    }


//    public  ZkUtil() {
//        // todo 初始化，与zk建立连接，注册监听路径，当配置有变化随时更新
//
//
//        //todo 先连接zk
//        //1.创建重试策略   第一个参数会话是睡眠多久重试一次。   第二个是重试次数
//        curatorFramework = getCuratorFramework();
//
//        //3.开始连接
//        curatorFramework.start();
//
//        //4.监听一个metaserver
//        metaServer=null;
//        //首先去主metaserver访问，如果主metaserver挂了，才去副节点
//        Stat stat = null;
//        try {
//            stat = cratorFramework.checkExists().forPath(masterMetaServer);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//        if(stat==null){
//            Stat secondaryStat = null;
//            try {
//                secondaryStat = curatorFramework.checkExists().forPath(secondaryMetaServer);
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//            if (secondaryStat!=null){
//                 metaServer=secondaryMetaServer;
//             }
//        }else {
//            metaServer=masterMetaServer;
//        }
//
//
//
//
////        //创建缓存节点,NodeCache用于监控一个ZooKeeper节点的变化，
////        NodeCache nodeCache = new NodeCache(curatorFramework, metaServer,false);
////        nodeCache.start(true);
////        //监控/iverson节点
////        nodeCache.getListenable().addListener(new NodeCacheListener() {
////            @Override
////            public void nodeChanged() throws Exception {
////                System.out.println("--------------------");
////                if (nodeCache.getCurrentData() != null) {
////                    System.out.println("路径为：" + nodeCache.getCurrentData().getPath());
////                    System.out.println("数据为：" + new String(nodeCache.getCurrentData().getData()));
////                    System.out.println("状态为：" + nodeCache.getCurrentData().getStat());
////                }else {
////                    System.out.println("此节点已被删除");
////                }
////                System.out.println("--------------------");
////            }
////        });
//    }



    public  static String getMetaServerData(){
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
