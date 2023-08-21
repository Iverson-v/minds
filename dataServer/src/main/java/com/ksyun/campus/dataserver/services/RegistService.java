package com.ksyun.campus.dataserver.services;

import com.ksyun.campus.dataserver.entity.DataServerInfo;
import com.ksyun.campus.dataserver.util.ZkUtil;
import com.ksyun.campus.dataserver.util.jaksonutils.JacksonUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;

@Component
public class RegistService {

//    @Override
//    public void run(ApplicationArguments args) throws Exception {
//        registToCenter();
//    }

    @Autowired
    private ZkUtil zkUtil;
    @Autowired
    private Environment environment;
    public void registToCenter() {
        // todo 将本实例信息注册至zk中心，包含信息 ip、port、capacity、rack、zone
        try {
            // Create a ZNode with the data
            CuratorFramework cf = zkUtil.getCuratorFramework();

            // Get properties
            String SynDataServersPrefix=environment.getProperty("synchronize-DataServers");// /SynDataServers
            String ip = environment.getProperty("server.ip");
            int port = environment.getProperty("server.port", Integer.class);
            int capacity = environment.getProperty("server.capacity", Integer.class);
            String rack = environment.getProperty("az.rack");
            String zone = environment.getProperty("az.zone");

            //先判断zk的/SynDataServers中有没有数据，有的话就同步。
            Stat statSyn = cf.checkExists().forPath(SynDataServersPrefix + "/" + ip + ":" + port);// /SynDataServers/localhost:9001
            if (statSyn!=null){
                //当前节点已经注册过了。去该目录下复制一份
                byte[] bytes = cf.getData().forPath(SynDataServersPrefix + "/" + ip + ":" + port);
                String json = new String(bytes);

                // ZNode path
                String dataServerPrefix=environment.getProperty("dataServer-prefix");// /dataServers
                //为当前ds注册一个节点，临时节点。
                cf.create().withMode(CreateMode.EPHEMERAL).forPath(dataServerPrefix+"/" + ip + ":" + port, json.getBytes());
                return;
            }

            //封装成pojo
            DataServerInfo dataServerInfo = new DataServerInfo(ip,port,capacity);

            //转成json
            String jsonStr = JacksonUtil.toJsonStr(dataServerInfo);

            // ZNode path
            String dataServerPrefix=environment.getProperty("dataServer-prefix");
            String path = dataServerPrefix+"/" + ip + ":" + port;




            //判断父节点是否为空，如果为空就创建父节点。永久节点
            Stat stat = cf.checkExists().forPath(dataServerPrefix);
            if(stat==null){
                cf.create().forPath(dataServerPrefix, "".getBytes());
            }

            //为当前ds注册一个节点，临时节点。
            cf.create().withMode(CreateMode.EPHEMERAL).forPath(path, jsonStr.getBytes());

        } catch (Exception e) {
            // Handle exceptions
            e.printStackTrace();
        }
    }




    public void reRegist() {
        // todo 将本实例重新注册

        CuratorFramework cf = zkUtil.getCuratorFramework();
        String path=environment.getProperty("dataServer-prefix")+"/"+environment.getProperty("server.ip")+":"
                +environment.getProperty("server.port");//  /dataServers/localhost:9001
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

    public List<Map<String, Integer>> getDslist() {
        return null;
    }




}
