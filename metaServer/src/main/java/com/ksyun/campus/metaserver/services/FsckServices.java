package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.domain.FileType;
import com.ksyun.campus.metaserver.domain.ReplicaData;
import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.entity.DataServerInfo;
import com.ksyun.campus.metaserver.entity.RestConsts;
import com.ksyun.campus.metaserver.entity.RestResult;
import com.ksyun.campus.metaserver.util.HttpClientUtils;
import com.ksyun.campus.metaserver.util.ZkUtil;
import com.ksyun.campus.metaserver.util.jaksonutils.JacksonUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class FsckServices {
    @Autowired
    private ZkUtil zkUtil;
    @Autowired
    private Environment environment;

    //@Scheduled(cron = "0 0 0 * * ?") // 每天 0 点执行
    @Scheduled(fixedRate = 1*60*1000) // 每隔 30 分钟执行一次  30*60*1000
    public void fsckTask() {
        //todo 因为有多个MetaServer（MS）实例在并行运行相同的定时任务时，会导致冲突和数据不一致的问题。使用分布式锁解决
        CuratorFramework cf = zkUtil.getCuratorFramework();
        InterProcessMutex lock = new InterProcessMutex(cf, "/lock_path");
        try {
            //调用试图获取一个分布式锁，并且允许最多等待30秒来获取这个锁。
            if (lock.acquire(30, TimeUnit.SECONDS)) {
                try {
                    // 恢复任务
                    runFsckTask();
                } finally {
                    //释放锁
                    lock.release();
                }
            }
        } catch (Exception e) {throw new RuntimeException(e);}
    }

    private void runFsckTask(){
        //递归查zk中所有文件。把结果放到列表中。
        CuratorFramework cf = zkUtil.getCuratorFramework();

        // todo 1.全量扫描文件列表
        List<StatInfo> files = scanAllFiles(cf,"/stat"); // 全量扫描文件列表，得到所有文件

        // todo 2.检查文件副本数量是否正常,不正常就要去zk更新文件副本数：3副本、2副本、单副本
        synZK(files);//根据所有文件去检查磁盘是否存在，然后更新ZK中文件元数据和DS元数据。


        //todo 3.恢复文件，如果恢复成功还要去更新zk中元数据
        for (StatInfo statInfo : files) {
            List<String> replicaDataList = checkReplicaCount(statInfo);// 获得拥有文件副本的节点  ip:port
            int currentReplicas =replicaDataList.size();

            if (currentReplicas>0&&currentReplicas < 3) {
                //todo------------------------------添加副本部分-------------------------------
                //从存在的dataServer复制一份数据。
                String dsNode = getAvailableFileDS(statInfo);  //dsNode=ip:port
                int needRepairCount=3-currentReplicas;

                //从zk中选择需要重新修复的机器，选择可用容量最大的ds
                List<String> needRepairDS;
                try {
                    List<String> names = cf.getChildren().forPath("/dataServers");
                    needRepairDS = names.stream().filter(s -> !replicaDataList.contains(s)).
                            map(s -> {
                                try {
                                    byte[] bytes = cf.getData().forPath("/dataServers" + "/" + s);
                                    String json = new String(bytes);
                                    return JacksonUtil.toBean(json, DataServerInfo.class);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);}}).
                            sorted((o1, o2) -> Math.toIntExact(o1.getUseCapacity() - o2.getUseCapacity())).
                            limit(needRepairCount).
                            map(o -> o.getHost() + ":" + o.getPort()).
                            collect(Collectors.toList());
                } catch (Exception e) {throw new RuntimeException(e);}
                String needRepairDSJsonStr = JacksonUtil.toJsonStr(needRepairDS);

                //发送给存在这个文件的ds，让他去传输文件。
                String url="http://"+dsNode+"/repair?path="+ statInfo.getPath();//http://localhost:9001/repair?path=/a/b/second.jpg
                try {
                    HttpClientUtils.postParameters(url,needRepairDSJsonStr,"application/json");//通知修复数据
                } catch (Exception e) {throw new RuntimeException(e);}


                //todo-------------------------------修改元数据部分------------------------------------
                //1.修改文件元数据和ds元数据。
                List<ReplicaData> currentRepliccaData=new ArrayList<>();
                for (String hostport:needRepairDS){
                    //ds的元数据要加上这个恢复的副本的容量,文件数量也要加一
                    long size = statInfo.getSize();
                    byte[] bytes = new byte[0];
                    try {
                        bytes = cf.getData().forPath("/dataServers/" + hostport);
                    } catch (Exception e) {throw new RuntimeException(e);}
                    DataServerInfo dataServerInfo = JacksonUtil.toBean(new String(bytes), DataServerInfo.class);
                    dataServerInfo.setUseCapacity(dataServerInfo.getUseCapacity()+size);
                    dataServerInfo.setFileTotal(dataServerInfo.getFileTotal()+1);
                    try {
                        cf.setData().forPath("/dataServers/"+hostport,JacksonUtil.toJsonStr(dataServerInfo).getBytes());
                    } catch (Exception e) {throw new RuntimeException(e);}



                    //这里副本信息写的path格式如下
                    //     "id" : "3026283a-3586-4b2c-a9b6-a0c80d146a4a",
                    //    "dsNode" : "localhost:9000",
                    //    "path" : "/a/b/second.jpg"
                    ReplicaData replicaData = new ReplicaData(UUID.randomUUID().toString(),hostport,statInfo.getPath());
                    currentRepliccaData.add(replicaData);
                }
                currentRepliccaData.addAll(statInfo.getReplicaData());//添加没被删除的副本信息组成三副本信息
                //currentRepliccaData加上要添加的副本的
                statInfo.setReplicaData(currentRepliccaData);
                try {cf.setData().forPath("/stat"+statInfo.getPath(),JacksonUtil.toJsonStr(statInfo).getBytes());
                } catch (Exception e) {throw new RuntimeException(e);}

                //todo ----------------------------同步/synDataservers元数据------------------------------------
                synchronizeDataServers();//同步到/synDataservers
            }
        }
    }


    //全量扫描所有文件，放到list中
    private List<StatInfo> scanAllFiles(CuratorFramework cf,String path) {
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
    //递归遍历子文件架
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

    //同步zk中元数据
    private void synZK(List<StatInfo> statInfos) {//全盘扫描所有文件，更新文件的元数据信息。
        CuratorFramework cf = zkUtil.getCuratorFramework();
        for (StatInfo statInfo:statInfos){
            List<ReplicaData> currentRepliccaData=new ArrayList<>();
            // 实现检查文件副本数量的逻辑
            String path = statInfo.getPath();//path='/a/b/second.jpg'
            List<ReplicaData> replicaDataList = statInfo.getReplicaData();//3副本信息。
            for (ReplicaData replicaData : replicaDataList) {
                //发送请求到ds，让他们检查文件是否存在
                String dsNode = replicaData.getDsNode();// ip:port
                String url="http://"+dsNode+"/checkReplicaCount?path="+path;//http://localhost:9001/checkReplicaCount?path=/a/b/second.jpg
                try {
                    String json = HttpClientUtils.get(url);
                    RestResult restResult = JacksonUtil.toBean(json, RestResult.class);
                    if (restResult.getCode()== RestConsts.DEFAULT_SUCCESS_CODE){
                        //这个如果缺失副本后，最新的副本信息，只保存还存在磁盘中的。
                        currentRepliccaData.add(replicaData);
                    }else {
                        //ds的元数据要减去这个消失的副本的容量,文件数量也要减一
                        long size = statInfo.getSize();
                        byte[] bytes = cf.getData().forPath("/dataServers/" + dsNode);
                        DataServerInfo dataServerInfo = JacksonUtil.toBean(new String(bytes), DataServerInfo.class);
                        dataServerInfo.setUseCapacity(dataServerInfo.getUseCapacity()-size);
                        dataServerInfo.setFileTotal(dataServerInfo.getFileTotal()-1);
                        cf.setData().forPath("/dataServers/"+dsNode,JacksonUtil.toJsonStr(dataServerInfo).getBytes());
                    }
                } catch (Exception e) {throw new RuntimeException(e);}
            }


            //向zk中覆盖文件的元数据
            //  "/a/b/second.jpg"
            statInfo.setReplicaData(currentRepliccaData);
            try {cf.setData().forPath("/stat"+path,JacksonUtil.toJsonStr(statInfo).getBytes());
            } catch (Exception e) {throw new RuntimeException(e);}
        }


    }

    //计算该文件正常的副本数
    private List<String> checkReplicaCount(StatInfo statInfo) {

        // 实现检查文件副本数量的逻辑
        List<String> list=new ArrayList<>();//String值是 ip：port的形式
        String path = statInfo.getPath();//path='/a/b/second.jpg'
        List<ReplicaData> replicaDataList = statInfo.getReplicaData();//3副本信息。
        for (ReplicaData replicaData : replicaDataList) {
            //发送请求到ds，让他们检查文件是否存在
            String dsNode = replicaData.getDsNode();// ip:port
            String url="http://"+dsNode+"/checkReplicaCount?path="+path;//http://localhost:9001/checkReplicaCount?path=/a/b/second.jpg
            try {
                String json = HttpClientUtils.get(url);
                RestResult restResult = JacksonUtil.toBean(json, RestResult.class);
                if (restResult.getCode()== RestConsts.DEFAULT_SUCCESS_CODE){
                    list.add(dsNode);
                }
            } catch (Exception e) {throw new RuntimeException(e);}
        }

        return list;
    }


    //向DS发送请求，判断这个DS是否存在这个文件。
    private String getAvailableFileDS(StatInfo file) {
        // 实现检查文件副本数量的逻辑
        String path = file.getPath();//path='/a/b/second.jpg'
        List<ReplicaData> replicaDataList = file.getReplicaData();//3副本信息。
        for (ReplicaData replicaData : replicaDataList) {
            //发送请求到ds，让他们检查文件是否存在
            String dsNode = replicaData.getDsNode();// ip:port
            String url="http://"+dsNode+"/checkReplicaCount?path="+path;//http://localhost:9001/checkReplicaCount?path=/a/b/second.jpg
            try {
                String json = HttpClientUtils.get(url);
                RestResult restResult = JacksonUtil.toBean(json, RestResult.class);
                if (restResult.getCode()== RestConsts.DEFAULT_SUCCESS_CODE){
                    //当前节点有副本，返回这个节点
                    return dsNode;
                }
            } catch (Exception e) {throw new RuntimeException(e);}
        }
        return null;
    }

    //同步到ZK的/synDataServers元数据
    private void synchronizeDataServers(){
        //dataServerUrlPrefix=dataServerUrlPrefix;//  /dataServers
        //如果dataServer挂了重新起会从这个文件下读取当前机器的元数据信息
        String SynDataServersPrefix=environment.getProperty("synchronize-DataServers");// /SynDataServers
        CuratorFramework cf = zkUtil.getCuratorFramework();
        List<String> list;
        try {list = cf.getChildren().forPath(environment.getProperty("dataServer-prefix"));}
        catch (Exception e) {throw new RuntimeException(e);}

        List<String> dataServerInfoJsonList = list.stream().map(s -> environment.getProperty("dataServer-prefix") + "/" + s).
                map(s -> {
                    try {
                        byte[] bytes = cf.getData().forPath(s);
                        String json = new String(bytes);
                        return json;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
        for (String dataServerInfoJson : dataServerInfoJsonList) {
            DataServerInfo dataServerInfo = JacksonUtil.toBean(dataServerInfoJson, DataServerInfo.class);
            String path=SynDataServersPrefix+"/"+dataServerInfo.getHost()+":"+dataServerInfo.getPort(); //  /SynDataServers/localhost:9001
            //判断当前path是否存在，不存在表示第一次同步，需要创建节点
            try {
                Stat stat = cf.checkExists().forPath(path);
                if(stat==null){
                    //不存在表示第一次同步，需要创建节点
                    cf.create().creatingParentsIfNeeded().forPath(path,dataServerInfoJson.getBytes());
                }else {
                    //存在直接修改节点
                    cf.setData().forPath(path,dataServerInfoJson.getBytes());
                }
            } catch (Exception e) {throw new RuntimeException(e);}

        }

    }
}
