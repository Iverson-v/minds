package com.ksyun.campus.client;

import com.ksyun.campus.client.entity.*;
import com.ksyun.campus.client.util.HttpClientUtils;
import com.ksyun.campus.client.util.ZkUtil;
import com.ksyun.campus.client.util.jaksonutils.JacksonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
public class FSOutputStream extends OutputStream {
    private String path;
    private String fileSystem;
    private String dataServerPrefix="/dataServers";
    private String synDataServerPrefix="/SynDataServers";

    private byte [] pack=new byte[8*1024*1024];//8MB一个包
    private long fileSize=0;
    private List<DataServerInfo> dataServerInfos=null;//发送给ds的元数据信息，取出来可以获得具体的ip地址
    int packPos=0;//写文件的位置

    public void setDataServerInfos(List<DataServerInfo> dataServerInfos) {
        this.dataServerInfos = dataServerInfos;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void setFileSystem(String fileSystem) {
        this.fileSystem = fileSystem;
    }

    @Override
    public void write(int b) throws IOException {
        byte[] bytes=new byte[1];
        bytes[0]=(byte) b;
        write(bytes);
    }

    @Override
    public void write(byte[] b) throws IOException {
        //todo 先写到本地。然后容量到了一定时候提交到ds；
        int offset = 0; // 要写入的数据在数组 b 中的起始位置

        while (offset < b.length) {//0<1024
            int length = Math.min(b.length - offset, pack.length - packPos); // 计算这次能写入多少数据

            // 将数据从数组 b 拷贝到数组 pack
            System.arraycopy(b, offset, pack, packPos, length);

            packPos += length; // 更新写入数据的位置
            offset += length; // 更新已处理的数据量

            // 检查 pack 是否已满
            if (packPos == pack.length) {
                // 如果 pack 已满，就将其发送到数据服务器
                sendPackToDataServer(pack,packPos,false);

                // 清空 pack
                packPos = 0;
            }
        }
    }

    private void sendPackToDataServer(byte[] pack, int length,boolean setFileTotal) {
        //累加文件大小
        fileSize+=length;
        // 这个方法用来将数据发送到数据服务器
        // 在这里，需要确保只发送数组 pack 中前 length 个元素

        byte[] subset = new byte[length];
        System.arraycopy(pack, 0, subset, 0, length);
        // 获取三个副本
        DataServerInfo dataServerInfo = dataServerInfos.get(0);
        FileAndMetaData fileAndMetaData=new FileAndMetaData(subset,
                dataServerInfo,dataServerInfos.get(1),dataServerInfos.get(2));
        String jsonStrfileAndMetaData = JacksonUtil.toJsonStr(fileAndMetaData);
        String url="http://"+dataServerInfo.getHost()+":"+dataServerInfo.getPort()+"/writeMaster?path="+path;
        //发送httpclient请求给ds
        try {
            HttpClientUtils.postSetHeaderAndSetBody(url, jsonStrfileAndMetaData, "application/json", "fileSystem", fileSystem);
//            RestResult restResult = JacksonUtil.toBean(jsonR, RestResult.class);
//            if (restResult.getCode()== RestConsts.DEFAULT_SUCCESS_CODE){
//                return true;
//            }else return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }



//        //当最后一个pack发送完执行这里，给储存了ds的服务器元数据的文件数量加一
//        if (setFileTotal){//设置文件数量加一
//            CuratorFramework cf = ZkUtil.getCuratorFramework();
//            String path1=dataServerPrefix+"/"+dataServerInfo.getHost()+":"+dataServerInfo.getPort();
//            Stat stat1=null;
//            try {stat1 = cf.checkExists().forPath(path1);} catch (Exception e) {throw new RuntimeException(e);}
//            if (stat1!=null){
//                //该副本存在，需要更新
//                byte[]bytes;
//                try {bytes  = cf.getData().forPath(path1);} catch (Exception e) {throw new RuntimeException(e);}
//                String json=new String(bytes);
//                DataServerInfo dataServer = JacksonUtil.toBean(json, DataServerInfo.class);
//                dataServer.setFileTotal(dataServer.getFileTotal()+1);
//                String jsonStr = JacksonUtil.toJsonStr(dataServer);
//                try {cf.setData().forPath(path1,jsonStr.getBytes());} catch (Exception e) {throw new RuntimeException(e);}
//            }
//
//            String path2=dataServerPrefix+"/"+dataServerInfos.get(1).getHost()+":"+dataServerInfos.get(1).getPort();
//            Stat stat2=null;
//            try {stat2 = cf.checkExists().forPath(path2);} catch (Exception e) {throw new RuntimeException(e);}
//            if (stat2!=null){
//                //该副本存在，需要更新
//                byte[]bytes;
//                try {bytes  = cf.getData().forPath(path2);} catch (Exception e) {throw new RuntimeException(e);}
//                String json=new String(bytes);
//                DataServerInfo dataServer = JacksonUtil.toBean(json, DataServerInfo.class);
//                dataServer.setFileTotal(dataServer.getFileTotal()+1);//更新文件数量
//                String jsonStr = JacksonUtil.toJsonStr(dataServer);
//                try {cf.setData().forPath(path2,jsonStr.getBytes());} catch (Exception e) {throw new RuntimeException(e);}
//            }
//
//            String path3=dataServerPrefix+"/"+dataServerInfos.get(2).getHost()+":"+dataServerInfos.get(2).getPort();
//            Stat stat3=null;
//            try {stat3 = cf.checkExists().forPath(path3);} catch (Exception e) {throw new RuntimeException(e);}
//            if (stat3!=null){
//                //该副本存在，需要更新
//                byte[]bytes;
//                try {bytes  = cf.getData().forPath(path3);} catch (Exception e) {throw new RuntimeException(e);}
//                String json=new String(bytes);
//                DataServerInfo dataServer = JacksonUtil.toBean(json, DataServerInfo.class);
//                dataServer.setFileTotal(dataServer.getFileTotal()+1);
//                String jsonStr = JacksonUtil.toJsonStr(dataServer);
//                try {cf.setData().forPath(path3,jsonStr.getBytes());} catch (Exception e) {throw new RuntimeException(e);}
//            }
//
//        }

    }


    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        //todo 先写到本地。然后容量到了一定时候提交到ds；
        byte[] result = new byte[len];
        System.arraycopy(b, off, result, 0, len);
        write(result);

    }

    @Override
    public void close() throws IOException {
        // 将剩余的数据发送到数据服务器
        if (packPos > 0) {
            sendPackToDataServer(pack, packPos,true);//最后一包需要发给ds，将文件数量加一
        }

        //todo--------------------------- 更新元数据----------------------------------
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!交给ms处理元数据更新
        //获得三副本信息



        //1.获取ZK中metaserver可用节点的节点值。
        String jsonStr = ZkUtil.getMetaServerData();
        if (jsonStr==null){
            //表示两台metaserver都挂了。
            return ;
        }
        //2.把metaserver节点值是json字符串，转化为对象
        MetaServerInfo metaServerInfo = JacksonUtil.toBean(jsonStr, MetaServerInfo.class);
        //3.拼接url，访问metaserver的地址
        String url="http://"+metaServerInfo.getHost()+":"+metaServerInfo.getPort()+"/write?path="+path+"&length="+fileSize;
        String jsonR=null;
        try {
            String dataServerInfosJson = JacksonUtil.toJsonStr(dataServerInfos);
            jsonR = HttpClientUtils.postParameters(url,dataServerInfosJson,"application/json");
        } catch (Exception e) {throw new RuntimeException(e);}
        //4.获取httpclient请求的结果，转化为对象
        RestResult restResult = JacksonUtil.toBean(jsonR, RestResult.class);
        if (restResult.getCode()==RestConsts.DEFAULT_SUCCESS_CODE){

        }
        //同步DataServers的数据到/SynDataServers节点下。
        synchronizeDataServers();
        System.out.println("文件已经成功写完！");
    }


    //todo ------------------------------同步DataServers的数据到/SynDataServers节点下----------------------
    private void synchronizeDataServers(){
        String dataServerUrlPrefix=dataServerPrefix;//  /dataServers
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

}
