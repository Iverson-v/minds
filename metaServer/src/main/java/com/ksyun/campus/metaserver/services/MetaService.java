package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.domain.FileType;
import com.ksyun.campus.metaserver.domain.ReplicaData;
import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.entity.DataServerInfo;
import com.ksyun.campus.metaserver.entity.FileAndMetaData;
import com.ksyun.campus.metaserver.entity.RestConsts;
import com.ksyun.campus.metaserver.entity.RestResult;
import com.ksyun.campus.metaserver.util.HttpClientUtils;
import com.ksyun.campus.metaserver.util.ZkUtil;
import com.ksyun.campus.metaserver.util.jaksonutils.JacksonUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Service
public class MetaService {

    @Autowired
    private ZkUtil zkUtil;

    @Autowired
    private Environment environment;



    //选出可用的ds，最多为3个
    public List<DataServerInfo> pickDataServer(){
        // todo 通过zk内注册的ds列表，选择出来一个ds，用来后续的wirte
        // 需要考虑选择ds的策略？负载

        //选择三个已使用容量最小的节点写副本，也就是可使用容量最大的三个节点写。
        CuratorFramework cf = zkUtil.getCuratorFramework();
        List<DataServerInfo> dataServerInfoList;
        try {
            List<String> jsonDataServersInfo=new ArrayList<>();
            List<String> list = cf.getChildren().forPath("/dataServers");
            for (String name:list) {
                byte[] bytes = cf.getData().forPath("/dataServers/" + name);
                String s = new String(bytes);
                jsonDataServersInfo.add(s);
            }


            dataServerInfoList = jsonDataServersInfo.stream()
                    .map(s -> JacksonUtil.toBean(s, DataServerInfo.class))
                    .sorted(((o1, o2) -> Math.toIntExact(o1.getUseCapacity() - o2.getUseCapacity())))
                    .limit(3)
                    .collect(Collectors.toList());


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return dataServerInfoList;

    }

    public boolean mkdir(String fileSystem,String path) {
        //todo 4个ds，选择哪三个节点进行创建文件夹。
        //在zk上查询4个节点的剩余容量。
        List<DataServerInfo> dataServerInfos = pickDataServer();
        //判断这个list装了几个dataserver，如果小于三个就不能创建。
        if(dataServerInfos.size()<3){
            return false;
        }
        //大于等于三个，此时才去创建,先得到第0个节点最为主节点。让这个节点去发送两个副本
        DataServerInfo dataServerInfo = dataServerInfos.get(0);//主副本
        dataServerInfos.remove(dataServerInfo);//删除主副本，那么list中还要2个副本。发给主副本。
        String url="http://"+dataServerInfo.getHost()+":"+dataServerInfo.getPort()+"/mkdirMaster?path="+path;
        //把剩下两个节点元数据包装成json字符串通过post请求发送给主节点
        String jsonStr = JacksonUtil.toJsonStr(dataServerInfos);
        //restJson是返回的Rest风格的数据。
        String restJson=null;
        try {
            restJson = HttpClientUtils.postSetHeaderAndSetBody(url, jsonStr,"application/json",
                    "fileSystem",fileSystem);//记得带上"application/json"参数！！

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //返回结果转化为对象
        RestResult restResult = JacksonUtil.toBean(restJson, RestResult.class);

        //如果返回成功就要往元数据记录信息。
        if (restResult.getCode()== RestConsts.DEFAULT_SUCCESS_CODE){


            //储存在zk上
            String stat = environment.getProperty("zookeepeer-stat-path");//值为：/stat
            CuratorFramework cf = zkUtil.getCuratorFramework();
            //把目录划分，在每一个节点上都储存元数据
            String[] split = path.split("/");
            List<String> everyDir = Arrays.stream(split).skip(1).collect(Collectors.toList());

            String head="";
            for (int i=0;i<everyDir.size();++i){
                try {
                    //在  /stat/aaa下创建永久节点
                    head=head+"/"+everyDir.get(i);//  /a/b
                    String headPath=stat+head;//最终在zookeeper中的地址    /stat/a/b
                    //判断当前文件夹是否存在，不存在就写元数据
                    Stat exist = cf.checkExists().forPath(headPath);
                    if (exist==null){//为空表示当前不存在。




                        //这里还要做一件事，就是往zk里面储存文件的元数据。包括数据的文件
                        ArrayList<ReplicaData> replicaDataList = new ArrayList<>();
                        replicaDataList.add(
                                new ReplicaData(UUID.randomUUID().toString(),
                                        dataServerInfo.getHost()+":"+dataServerInfo.getPort(),
                                        head));
                        replicaDataList.add(
                                new ReplicaData(UUID.randomUUID().toString(),
                                        dataServerInfos.get(0).getHost()+":"+dataServerInfos.get(0).getPort(),
                                        head));
                        replicaDataList.add(
                                new ReplicaData(UUID.randomUUID().toString(),
                                        dataServerInfos.get(1).getHost()+":"+dataServerInfos.get(1).getPort(),
                                        head));
                        //元数据中，文件夹大小为0
                        StatInfo statInfo = new StatInfo(head,0,System.currentTimeMillis(), FileType.Directory,replicaDataList);
                        //转换为json字符串
                        String jsonStrStatInfo = JacksonUtil.toJsonStr(statInfo);






                        cf.create().creatingParentsIfNeeded().forPath(headPath,jsonStrStatInfo.getBytes());
                    }

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }





            return true;
        }else return false;




        //todo 通过HTTPClient协议，发送请求到第一个ds作为主副本，并通知它同步到另外两个副本
    }

    public boolean delete(String fileSystem, String path) {//path=/a/b/c
        //1.查询对应路径的元数据
        CuratorFramework cf = zkUtil.getCuratorFramework();
        String headPath=environment.getProperty("zookeepeer-stat-path")+path;//   /stat/a/b/c
        //2.判断当前路径是否存在
        try {Stat stat = cf.checkExists().forPath(headPath);
            if (stat==null){
                return false;//该文件不存在
            }
        } catch (Exception e) {throw new RuntimeException(e);}

        //3.得到当前文件或文件夹的大小。
        byte[] bytes;
        try {bytes = cf.getData().forPath(headPath);} catch (Exception e) {throw new RuntimeException(e);}
        String json=new String(bytes);
        StatInfo statInfo = JacksonUtil.toBean(json, StatInfo.class);
        long needSubstractSize = statInfo.getSize();//这个属性，之前的文件夹大小要减去这个值


        //第一个异步任务
        CompletableFuture<Boolean> completableFuture1 = CompletableFuture.supplyAsync(()-> {
            //4.删除zk中此文件节点的元数据
            try {cf.delete().deletingChildrenIfNeeded().forPath(headPath);} catch (Exception e) {throw new RuntimeException(e);}

            //5.递归修改zk中此文件之前的目录元数据
            String[] split = path.split("/");
            if (split.length==0){
                //todo 表示删除所有文件。
            }
            List<String> collect = Arrays.stream(split).skip(1).limit(Math.max(split.length-2,0)).collect(Collectors.toList());//  {a,b}
            String tempPath=environment.getProperty("zookeepeer-stat-path");// /stat
            for (String str :collect) {
                tempPath+="/"+str;
                //修改文件元数据
                try {
                    byte[] bytesTemp = cf.getData().forPath(tempPath);
                    String jsonTemp = new String(bytesTemp);
                    StatInfo statInfoTemp = JacksonUtil.toBean(jsonTemp, StatInfo.class);
                    statInfoTemp.setSize(statInfoTemp.getSize()-needSubstractSize);//设置文件夹为新的容量
                    statInfoTemp.setMtime(System.currentTimeMillis());//设置当前时间
                    String jsonTempNew = JacksonUtil.toJsonStr(statInfoTemp);
                    cf.setData().forPath(tempPath,jsonTempNew.getBytes());//重新设置文件元数据
                } catch (Exception e) {throw new RuntimeException(e);}
            }
            //异步执行完返回true
            return true;
            });

        //6.获取所有dataServer元数据
        List<DataServerInfo> dataServerInfoList;
        try {
            String dateServerPrefix=environment.getProperty("dataServer-prefix");//  /dataServers
            List<String> listChildren = cf.getChildren().forPath(dateServerPrefix);
            dataServerInfoList = listChildren.stream().
                    map(name -> {
                        try {
                            return new String(cf.getData().forPath(dateServerPrefix + "/" + name));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).
                    map(s -> JacksonUtil.toBean(s, DataServerInfo.class)).
                    collect(Collectors.toList());
        } catch (Exception e) {throw new RuntimeException(e);}


        //7.异步发送给所有的dataserver，让他们删除对应文件或者文件夹
        Future<Boolean>[] flags = new Future[dataServerInfoList.size()];
        for (int i = 0; i < dataServerInfoList.size(); i++) {
            flags[i] = deleteSendToDS(dataServerInfoList, path, i, fileSystem);
        }


        //8.返回结果。强一致性！
        if(!completableFuture1.join()){
            return false;
        }
        for (Future<Boolean> flag : flags) {
            try {
                if (!flag.get()) { // 此处会阻塞主线程，直到结果可用
                    return false;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        //没出错，返回成功
        return true;
    }

    //异步执行的方法
    @Async
    public Future<Boolean> deleteSendToDS(List<DataServerInfo> dataServerInfoList, String path, int i, String fileSystem) {
        String url = "http://" + dataServerInfoList.get(i).getHost() + ":" + dataServerInfoList.get(i).getPort() + "/delete?path=" + path;
        boolean flag=false;
        try {
            String json = HttpClientUtils.getAndSetHeader(url, "fileSystem", fileSystem);
            RestResult restResult = JacksonUtil.toBean(json, RestResult.class);
            if (restResult.getCode()==RestConsts.DEFAULT_SUCCESS_CODE){
                flag=true;
            }
        } catch (Exception e) { throw new RuntimeException(e); }
        return new AsyncResult<>(flag);
    }



    public boolean commitWrite(String path, List<DataServerInfo> dataServerInfos,long length) {

        DataServerInfo currentDataServerInfo = dataServerInfos.get(0);
        DataServerInfo dataServerInfo1 = dataServerInfos.get(1);
        DataServerInfo dataServerInfo2 = dataServerInfos.get(2);


        //todo ------------------------------如果写成功，则需要向zk写文件元数据---------------------------------------

        String stat = environment.getProperty("zookeepeer-stat-path");//值为：/stat
        CuratorFramework cf = zkUtil.getCuratorFramework();
        //把目录划分，在每一个节点上都储存元数据
        String[] split = path.split("/");//split={"","a","b","love.jpg"}
        List<String> everyDir = Arrays.stream(split).skip(1).collect(Collectors.toList());//everyDir={"a","b","love.jpg"}
        String head="";

        for (int i=0;i<everyDir.size();++i) {
            //在  /stat/aaa下创建永久节点
            head = head + "/" + everyDir.get(i);//  /a/b
            String headPath = stat + head;//最终在zookeeper中的地址    /stat/a/b

            //第一种情况，最后一层，写文件元数据
            if(i==everyDir.size()-1){//最后一层，写文件元数据
                //往zk里面储存文件的元数据。
                ArrayList<ReplicaData> replicaDataList = new ArrayList<>();
                replicaDataList.add(
                        new ReplicaData(UUID.randomUUID().toString(),
                                currentDataServerInfo.getHost()+":"+currentDataServerInfo.getPort(),
                                head));
                replicaDataList.add(
                        new ReplicaData(UUID.randomUUID().toString(),
                                dataServerInfo1.getHost()+":"+dataServerInfo1.getPort(),
                                head));
                replicaDataList.add(
                        new ReplicaData(UUID.randomUUID().toString(),
                                dataServerInfo2.getHost()+":"+dataServerInfo2.getPort(),
                                head));


                //元数据中，文件大小为length
                StatInfo statInfo = new StatInfo(head,length,System.currentTimeMillis(), FileType.File,replicaDataList);
                //转换为json字符串
                String jsonStrStatInfo = JacksonUtil.toJsonStr(statInfo);
                try {
                    cf.create().creatingParentsIfNeeded().forPath(headPath,jsonStrStatInfo.getBytes());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                break;//这种情况完成就不循环
//                //判断文件是否已经存在
//                Stat fileStat=null;
//                try {fileStat = cf.checkExists().forPath(headPath);//  /stat/a/iverson/love.jpg
//                } catch (Exception e) {throw new RuntimeException(e);}
//
//                if(fileStat==null){//第一次创建文件，
//                    //元数据中，文件大小为length
//                    StatInfo statInfo = new StatInfo(head,length,System.currentTimeMillis(), FileType.File,replicaDataList);
//                    //转换为json字符串
//                    String jsonStrStatInfo = JacksonUtil.toJsonStr(statInfo);
//                    try {
//                        cf.create().creatingParentsIfNeeded().forPath(headPath,jsonStrStatInfo.getBytes());
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                else {
//                    //已经创建文件了，需要把之前文件大小叠加
//                    byte[] bytes=null;
//                    try {bytes= cf.getData().forPath(headPath);} catch (Exception e) {throw new RuntimeException(e);}
//                    String jsonStr = new String(bytes);
//                    StatInfo statInfo = JacksonUtil.toBean(jsonStr, StatInfo.class);
//                    statInfo.setSize(statInfo.getSize()+length);//修改文件大小
//                    statInfo.setMtime(System.currentTimeMillis());//修改最后修改时间
//                    //转换为json字符串
//                    String jsonStrStatInfo = JacksonUtil.toJsonStr(statInfo);
//                    try {cf.setData().forPath(headPath,jsonStrStatInfo.getBytes());} catch (Exception e) {throw new RuntimeException(e);}
//                }


            }

            //第二种情况，遍历目录，重写元数据
            try {
                //获取元数据中的信息，修改时间和文件大小
                byte[] bytes = cf.getData().forPath(headPath);
                String json = new String(bytes);
                StatInfo statInfo = JacksonUtil.toBean(json, StatInfo.class);
                statInfo.setMtime(System.currentTimeMillis());
                statInfo.setSize(statInfo.getSize()+length);
                //转换为json字符串
                String jsonStrStatInfo = JacksonUtil.toJsonStr(statInfo);
                //重新赋值
                cf.setData().forPath(headPath, jsonStrStatInfo.getBytes());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }



        //todo ------------------------------------向zk还要更新ds的信息----------------------------------------
        String path1=environment.getProperty("dataServer-prefix")+"/"+currentDataServerInfo.getHost()+":"+currentDataServerInfo.getPort();
        Stat stat1=null;
        try {stat1 = cf.checkExists().forPath(path1);} catch (Exception e) {throw new RuntimeException(e);}
        if (stat1!=null){
            //该副本存在，需要更新
            //currentDataServerInfo.setFileTotal(currentDataServerInfo.getFileTotal()+1);
            byte[]bytes;
            try {bytes  = cf.getData().forPath(path1);} catch (Exception e) {throw new RuntimeException(e);}
            String json=new String(bytes);
            DataServerInfo dataServerInfo = JacksonUtil.toBean(json, DataServerInfo.class);
            dataServerInfo.setFileTotal(dataServerInfo.getFileTotal()+1);
            dataServerInfo.setUseCapacity(dataServerInfo.getUseCapacity()+length);
            String jsonStr = JacksonUtil.toJsonStr(dataServerInfo);
            try {cf.setData().forPath(path1,jsonStr.getBytes());} catch (Exception e) {throw new RuntimeException(e);}
        }

        String path2=environment.getProperty("dataServer-prefix")+"/"+dataServerInfo1.getHost()+":"+dataServerInfo1.getPort();
        Stat stat2=null;
        try {stat2 = cf.checkExists().forPath(path2);} catch (Exception e) {throw new RuntimeException(e);}
        if (stat2!=null){
            //该副本存在，需要更新
            // dataServerInfo1.setFileTotal(dataServerInfo1.getFileTotal()+1);//更新文件数量
            byte[]bytes;
            try {bytes  = cf.getData().forPath(path2);} catch (Exception e) {throw new RuntimeException(e);}
            String json=new String(bytes);
            DataServerInfo dataServerInfo = JacksonUtil.toBean(json, DataServerInfo.class);
            dataServerInfo.setUseCapacity(dataServerInfo.getUseCapacity()+length);//更新使用容量
            dataServerInfo.setFileTotal(dataServerInfo.getFileTotal()+1);
            String jsonStr = JacksonUtil.toJsonStr(dataServerInfo);
            try {cf.setData().forPath(path2,jsonStr.getBytes());} catch (Exception e) {throw new RuntimeException(e);}
        }

        String path3=environment.getProperty("dataServer-prefix")+"/"+dataServerInfo2.getHost()+":"+dataServerInfo2.getPort();
        Stat stat3=null;
        try {stat3 = cf.checkExists().forPath(path3);} catch (Exception e) {throw new RuntimeException(e);}
        if (stat3!=null){
            //该副本存在，需要更新
            //dataServerInfo2.setFileTotal(dataServerInfo2.getFileTotal()+1);
            byte[]bytes;
            try {bytes  = cf.getData().forPath(path3);} catch (Exception e) {throw new RuntimeException(e);}
            String json=new String(bytes);
            DataServerInfo dataServerInfo = JacksonUtil.toBean(json, DataServerInfo.class);
            dataServerInfo.setUseCapacity(dataServerInfo.getUseCapacity()+length);
            dataServerInfo.setFileTotal(dataServerInfo.getFileTotal()+1);
            String jsonStr = JacksonUtil.toJsonStr(dataServerInfo);
            try {cf.setData().forPath(path3,jsonStr.getBytes());} catch (Exception e) {throw new RuntimeException(e);}
        }


        return true;
    }


    //根据文件路径（或者文件夹）返回文件的状态信息。
    public StatInfo getFileStats(String path) {
        CuratorFramework curatorFramework = zkUtil.getCuratorFramework();
        String prefixNode=environment.getProperty("zookeepeer-stat-path");
        try {
            Stat stat = curatorFramework.checkExists().forPath(prefixNode+path);
            if (stat==null){
                //当前文件不存在。
                System.out.println(path+"文件不存在,需要创建。");
                return null;
            }else {
                byte[] bytes = curatorFramework.getData().forPath(prefixNode + path);
                String s = new String(bytes);
                StatInfo statInfo = JacksonUtil.toBean(s, StatInfo.class);
                return statInfo;

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public List<StatInfo> listFileStats(String path) {
        String prefixNode=environment.getProperty("zookeepeer-stat-path");
        List<StatInfo> statInfos;
        CuratorFramework cf = zkUtil.getCuratorFramework();
        try {
            List<String> list = cf.getChildren().forPath(prefixNode + path);
            statInfos = list.stream().
                    map(s ->prefixNode + path+"/"+s ).
                    map(s -> {
                        try {
                            byte[] bytes = cf.getData().forPath(s);
                            String json = new String(bytes);
                            return json;
                        } catch (Exception e) {throw new RuntimeException(e);}
                    }).
                    map(s -> JacksonUtil.toBean(s, StatInfo.class)).collect(Collectors.toList());
        } catch (Exception e) {throw new RuntimeException(e);}
        return statInfos;
    }



    //递归列出所有文件的子文件
//    public List<StatInfo> listFileStats(String path) {
//        String prefixNode=environment.getProperty("zookeepeer-stat-path");
//        List<StatInfo> statInfos = new ArrayList<>();
//        collectFileStats(prefixNode+path, statInfos);//递归遍历
//        return statInfos;
//    }
//
//    private void collectFileStats(String path, List<StatInfo> statInfos) {
//        try {
//            CuratorFramework curatorFramework = zkUtil.getCuratorFramework();
//
//            if (curatorFramework.checkExists().forPath(path) != null) {
//                List<String> children = curatorFramework.getChildren().forPath(path);
//
//                for (String child : children) {
//                    String childPath = path + (path.endsWith("/") ? "" : "/") + child;
//
//                    byte[] bytes = curatorFramework.getData().forPath(childPath);
//                    String json = new String(bytes);
//                    StatInfo statInfo = JacksonUtil.toBean(json, StatInfo.class);
//                    statInfos.add(statInfo);
//
//                    collectFileStats(childPath, statInfos);
//                }
//            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
}
