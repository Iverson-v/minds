package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.entity.MetaServerInfo;
import com.ksyun.campus.metaserver.util.ZkUtil;
import com.ksyun.campus.metaserver.util.jaksonutils.JacksonUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class RegistService {
    @Autowired
    private ZkUtil zkUtil;

    @Autowired
    private Environment environment;



    public void registToCenter(){
        // todo 将本实例信息注册至zk中心，包含信息 ip、port

        try {
            // Get properties
            String ip = environment.getProperty("server.ip");
            int port = environment.getProperty("server.port", Integer.class);

            //封装成pojo
            MetaServerInfo metaServerInfo = new MetaServerInfo(ip,port);

            //转成json
            String jsonStr = JacksonUtil.toJsonStr(metaServerInfo);

            // ZNode path
            String metaServerPrefix=environment.getProperty("metaServer-prefix");
            String path = metaServerPrefix+"/" + ip + ":" + port;

            // Create a ZNode with the data
            CuratorFramework cf = zkUtil.getCuratorFramework();

            //判断父节点是否为空，如果为空就创建父节点。永久节点
            Stat stat = cf.checkExists().forPath(metaServerPrefix);
            if(stat==null){
                cf.create().forPath(metaServerPrefix, "".getBytes());
            }

            //为当前ms注册一个节点，临时节点。
            cf.create().withMode(CreateMode.EPHEMERAL).forPath(path, jsonStr.getBytes());

        } catch (Exception e) {
            // Handle exceptions
            e.printStackTrace();
        }




    }

    public void reRegist() {
        // todo 将本实例重新注册

        CuratorFramework cf = zkUtil.getCuratorFramework();
        String path=environment.getProperty("metaServer-prefix")+"/"+environment.getProperty("server.ip")+":"
                +environment.getProperty("server.port");//  /metaServers/localhost:8001
        //创建缓存节点,NodeCache用于监控一个ZooKeeper节点的变化，
        NodeCache nodeCache = new NodeCache(cf, path,false);
        try {
            nodeCache.start(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //监控/iverson节点
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                if (nodeCache.getCurrentData() == null) {
                    System.out.println("重新注册中！");
                    registToCenter();//重新注册
                }
            }
        });
    }
}
