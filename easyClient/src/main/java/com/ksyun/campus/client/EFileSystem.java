package com.ksyun.campus.client;

import com.ksyun.campus.client.domain.ClusterInfo;
import com.ksyun.campus.client.domain.ReplicaData;
import com.ksyun.campus.client.domain.StatInfo;
import com.ksyun.campus.client.entity.DataServerInfo;
import com.ksyun.campus.client.entity.MetaServerInfo;
import com.ksyun.campus.client.entity.RestConsts;
import com.ksyun.campus.client.entity.RestResult;
import com.ksyun.campus.client.util.HttpClientUtils;
import com.ksyun.campus.client.util.ZkUtil;
import com.ksyun.campus.client.util.jaksonutils.JacksonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class EFileSystem extends FileSystem{
    private String statPrefix="/stat";
    private String dataServerUrlPrefix="/dataServers";
    private String synDataServerPrefix="/SynDataServers";
    private String masterMetaServerUrl="/metaServers/localhost:8000";
    private String slaveMetaServerUrl="/metaServers/localhost:8001";

    private String fileName="";
    public EFileSystem() {
    }
    public  EFileSystem(String fileSystem){
        super.fileSystem=fileSystem;
    }




    //读文件
    public FSInputStream open(String path) {//
        StatInfo fileStats = getFileStats(path);
        if (fileStats==null){
            System.out.println("该文件不存在！无法读取！");
            return null;
        }
        List<ReplicaData> list = fileStats.getReplicaData();
        Random random=new Random();
        ReplicaData replicaData=list.get(random.nextInt(3));
        FSInputStream fsInputStream = new FSInputStream();
        fsInputStream.setReplicaData(replicaData);
        fsInputStream.setFileSystem(fileSystem);
        fsInputStream.setPath(path);
        return fsInputStream;
    }



    //写文件
    public FSOutputStream create(String path){
        //todo 这里要先判断这个路径是否已经存在，如果存在就不创建返回错误，如果不存在就创建。去ms去查询

        //1.获取ZK中metaserver可用节点的节点值。
        String jsonStr = ZkUtil.getMetaServerData();
        if (jsonStr==null){
            //表示两台metaserver都挂了。
            System.out.println("MetaServer无法连接！");
            return null;
        }

        //判断该文件是否存在，存在的话直接返回错误
        //判断该地址是否存在
        CuratorFramework cf = ZkUtil.getCuratorFramework();
        try {
            Stat stat = cf.checkExists().forPath(statPrefix + path);
            if (stat!=null){
                //该文件已存在，返回错误
                System.out.println("该文件已经存在！");
                return null;
            }
        } catch (Exception e) {throw new RuntimeException(e);}
//        StatInfo fileStats = getFileStats(path);
//        if (fileStats!=null){
//            //该文件已存在，返回错误
//            System.out.println("该文件已经存在！");
//            return null;
//        }

        //2.先去创建文件夹
        String[] split = path.split("/");//path=/a/iverson/love.jpg
        int size=split.length;
        if (size>2){//当分割之后大于2才创建文件夹
            List<String> collect = Arrays.stream(split).skip(1).limit(size - 2).collect(Collectors.toList());
            String head="";
            for (String s : collect) {
                s="/"+s;
                head=head+s;
            }
            //判断当前文件夹是否存在。
            mkdir(head);//创建文件夹
        }

        //发送httpclient请求，获取容量最小的三个节点。
        //3.把metaserver节点值是json字符串，转化为对象
        MetaServerInfo metaServerInfo = JacksonUtil.toBean(jsonStr, MetaServerInfo.class);
        //4.拼接url，访问metaserver的创建目录的地址
        String url="http://"+metaServerInfo.getHost()+":"+metaServerInfo.getPort()+"/create?path="+path;
        String jsonR=null;
        try {

            jsonR = HttpClientUtils.getAndSetHeader(url,"fileSystem",fileSystem);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //4.获取httpclient请求的结果，转化为对象，获得容量最小的三个节点。如果可用的小于三个，这里dataServerInfos会少于三个，
        List<DataServerInfo> dataServerInfos = JacksonUtil.toBeanList(jsonR, DataServerInfo.class);
        if (dataServerInfos.size()<3){
            //ds数量少于三天，不允许写文件，
            System.err.println("可用ds数量少于三台，不允许写文件");
            return null;
        }

        FSOutputStream fsOutputStream = new FSOutputStream();
        fsOutputStream.setDataServerInfos(dataServerInfos);
        fsOutputStream.setPath(path);
        fsOutputStream.setFileSystem(fileSystem);
        return fsOutputStream;

    }


    public boolean mkdir(String path){
        //todo 这里要先判断这个路径是否已经存在，如果存在就不创建返回错误，如果不存在就创建。去ms去查询

        //1.获取ZK中metaserver可用节点的节点值。

        String jsonStr = ZkUtil.getMetaServerData();
        if (jsonStr==null){
            //表示两台metaserver都挂了。
            System.out.println("MetaServer无法连接！");
            return false;
        }

        //2.把metaserver节点值是json字符串，转化为对象
        MetaServerInfo metaServerInfo = JacksonUtil.toBean(jsonStr, MetaServerInfo.class);
        //3.拼接url，访问metaserver的创建目录的地址
        String url="http://"+metaServerInfo.getHost()+":"+metaServerInfo.getPort()+"/mkdir?path="+path;
        String jsonR=null;
        try {

            jsonR = HttpClientUtils.getAndSetHeader(url,"fileSystem",fileSystem);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //4.获取httpclient请求的结果，转化为对象
        RestResult restResult = JacksonUtil.toBean(jsonR, RestResult.class);

        //5.如果code==500表示创建失败，否则创建成功
        int code = restResult.getCode();
        if (code== RestConsts.DEFAULT_FAILURE_CODE){
            //创建失败
            return false;
        }else return true;
    }




    //删除文件
    public boolean delete(String path){
        //1.获取ZK中metaserver可用节点的节点值。
        String jsonStr = ZkUtil.getMetaServerData();
        if (jsonStr==null){
            //表示两台metaserver都挂了。
            return false;
        }

        //2.把metaserver节点值是json字符串，转化为对象
        MetaServerInfo metaServerInfo = JacksonUtil.toBean(jsonStr, MetaServerInfo.class);
        //3.拼接url，访问metaserver的创建目录的地址
        String url="http://"+metaServerInfo.getHost()+":"+metaServerInfo.getPort()+"/delete?path="+path;
        String jsonR=null;
        try {

            jsonR = HttpClientUtils.getAndSetHeader(url,"fileSystem",fileSystem);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //4.获取httpclient请求的结果，转化为对象
        RestResult restResult = JacksonUtil.toBean(jsonR, RestResult.class);
        //5.如果code==500表示删除失败，否则删除成功
        int code = restResult.getCode();
        if (code== RestConsts.DEFAULT_FAILURE_CODE){
            //创建失败
            return false;
        }else {
            //同步DataServers的数据到/SynDataServers节点下。
            synchronizeDataServers();
            return true;
        }
    }




    //根据文件路径（或者文件夹）返回文件的状态信息。
    public StatInfo getFileStats(String path){
        //判断该地址是否存在
        CuratorFramework cf = ZkUtil.getCuratorFramework();
        try {
            Stat stat = cf.checkExists().forPath(statPrefix + path);
            if (stat==null){
                System.out.println("该path下没有文件。");
                return null;
            }
        } catch (Exception e) {throw new RuntimeException(e);}

        //1.获取ZK中metaserver可用节点的节点值。
        String jsonStr = ZkUtil.getMetaServerData();
        if (jsonStr==null){
            //表示两台metaserver都挂了。
            System.out.println("metaServer不可用！");
            return null;
        }

        //2.把metaserver节点值是json字符串，转化为对象
        MetaServerInfo metaServerInfo = JacksonUtil.toBean(jsonStr, MetaServerInfo.class);
        //3.拼接url，访问metaserver的创建目录的地址
        String url="http://"+metaServerInfo.getHost()+":"+metaServerInfo.getPort()+"/stats?path="+path;
        String jsonR=null;
        try {
            jsonR = HttpClientUtils.get(url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //4.获取httpclient请求的结果，转化为对象
        StatInfo statInfo = JacksonUtil.toBean(jsonR, StatInfo.class);
        return statInfo;

    }



    public List<StatInfo> listFileStats(String path){
        //判断该地址是否存在
        CuratorFramework cf = ZkUtil.getCuratorFramework();
        try {
            Stat stat = cf.checkExists().forPath(statPrefix + path);
            if (stat==null){
                System.out.println("该目录不存在！");
                return null;
            }
        } catch (Exception e) {throw new RuntimeException(e);}

        //1.获取ZK中metaserver可用节点的节点值。
        String jsonStr = ZkUtil.getMetaServerData();
        if (jsonStr==null){
            //表示两台metaserver都挂了。
            System.out.println("metaServer不可用！");
            return null;
        }

        //2.把metaserver节点值是json字符串，转化为对象
        MetaServerInfo metaServerInfo = JacksonUtil.toBean(jsonStr, MetaServerInfo.class);
        //3.拼接url，访问metaserver的创建目录的地址
        String url="http://"+metaServerInfo.getHost()+":"+metaServerInfo.getPort()+"/listdir?path="+path;
        String jsonR=null;
        try {
            jsonR = HttpClientUtils.get(url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //4.获取httpclient请求的结果，转化为对象
        List<StatInfo> list = JacksonUtil.toBeanList(jsonR, StatInfo.class);
        return list;
    }


    //同步DataServers的数据到/SynDataServers节点下。
    private void synchronizeDataServers(){
         //dataServerUrlPrefix=dataServerUrlPrefix;//  /dataServers
        //如果dataServer挂了重新起会从这个文件下读取当前机器的元数据信息
        String SynDataServersPrefix=synDataServerPrefix;// /SynDataServers
        CuratorFramework cf = ZkUtil.getCuratorFramework();
        List<String> list;
        try {list = cf.getChildren().forPath(dataServerUrlPrefix);} catch (Exception e) {throw new RuntimeException(e);}

        List<String> dataServerInfoJsonList = list.stream().map(s -> dataServerUrlPrefix + "/" + s).
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


    public ClusterInfo getClusterInfo(){
        CuratorFramework cf = ZkUtil.getCuratorFramework();
//        String masterMetaServerUrl="/metaServers/localhost:8000";
//        String slaveMetaServerUrl="/metaServers/localhost:8001";
        byte[] bytesMaster = new byte[0];
        byte[] bytesSlave = new byte[0];
        ClusterInfo.MetaServerMsg masterMetaServer = null;
        ClusterInfo.MetaServerMsg slaveMetaServer = null;
        try {
            //如果该服务没有注册，应该为null。
            Stat statMaster = cf.checkExists().forPath(masterMetaServerUrl);
            if (statMaster!=null){
                bytesMaster = cf.getData().forPath(masterMetaServerUrl);
                String jsonMaster=new String(bytesMaster);
                masterMetaServer = JacksonUtil.toBean(jsonMaster, ClusterInfo.MetaServerMsg.class);
            }
            Stat statSlave = cf.checkExists().forPath(slaveMetaServerUrl);
            if (statSlave!=null){
                bytesSlave = cf.getData().forPath(slaveMetaServerUrl);
                String jsonSlave=new String(bytesSlave);
                slaveMetaServer = JacksonUtil.toBean(jsonSlave, ClusterInfo.MetaServerMsg.class);
            }
        } catch (Exception e) {throw new RuntimeException(e);}


        //String dataServerUrlPrefix="/dataServers";
        List<String> list;
        try {list = cf.getChildren().forPath(dataServerUrlPrefix);} catch (Exception e) {throw new RuntimeException(e);}

        List<ClusterInfo.DataServerMsg> dataServerMsgs = list.stream().map(s -> dataServerUrlPrefix + "/" + s).
                map(s -> {
                    try {
                        byte[] bytes = cf.getData().forPath(s);
                        String json = new String(bytes);
                        return json;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).map(s -> JacksonUtil.toBean(s, ClusterInfo.DataServerMsg.class)).collect(Collectors.toList());


        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.setMasterMetaServer(masterMetaServer);
        clusterInfo.setSlaveMetaServer(slaveMetaServer);
        clusterInfo.setDataServer(dataServerMsgs);


        return clusterInfo;
    }
}
