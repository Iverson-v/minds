package com.ksyun.campus.metaserver.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class ZkUtil {

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
}
