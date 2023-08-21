package com.ksyun.campus.dataserver.services;
import com.ksyun.campus.dataserver.domain.FileType;
import com.ksyun.campus.dataserver.domain.ReplicaData;
import com.ksyun.campus.dataserver.domain.StatInfo;
import com.ksyun.campus.dataserver.entity.*;
import com.ksyun.campus.dataserver.util.HttpClientUtils;
import com.ksyun.campus.dataserver.util.ZkUtil;
import com.ksyun.campus.dataserver.util.jaksonutils.JacksonUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;


@Service
public class DataService {
    private Map<String, FileInputStream> fileStreams = new HashMap<>();
    @Autowired
    private Environment environment;
    @Autowired
    private ZkUtil zkUtil;


    public boolean write(FileAndMetaData fileAndMetaData, String fileSystem, String path){
        //todo 写本地
        //todo 调用远程ds服务写接口，同步副本，已达到多副本数量要求
        //todo 选择策略，按照 az rack->zone 的方式选取，将三副本均分到不同的az下
        //todo 支持重试机制
        //todo 返回三副本位置
        byte[] fileData = fileAndMetaData.getFileData();
        long length=fileData.length;//单位字节
        DataServerInfo currentDataServerInfo = fileAndMetaData.getMetaData();
        DataServerInfo dataServerInfo1 = fileAndMetaData.getMetaData1();
        DataServerInfo dataServerInfo2 = fileAndMetaData.getMetaData2();

        //1.异步分发给另外两个节点创建
        String url1="http://"+dataServerInfo1.getHost()+":"+dataServerInfo1.getPort()+"/write?path="+path;
        String url2="http://"+dataServerInfo2.getHost()+":"+dataServerInfo2.getPort()+"/write?path="+path;
        //第一个异步线程启动
        CompletableFuture<Boolean> completableFuture1 = CompletableFuture.supplyAsync(new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                try {
                    String jsonR = HttpClientUtils.postDataAndSetHeader(url1,fileData,"fileSystem",fileSystem);
                    RestResult restResult = JacksonUtil.toBean(jsonR, RestResult.class);
                    if (restResult.getCode()== RestConsts.DEFAULT_SUCCESS_CODE){
                        return true;
                    }else return false;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        });

        //第二个异步线程启动
        CompletableFuture<Boolean> completableFuture2 = CompletableFuture.supplyAsync(new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                try {
                    String jsonR = HttpClientUtils.postDataAndSetHeader(url2,fileData,"fileSystem",fileSystem);
                    RestResult restResult = JacksonUtil.toBean(jsonR, RestResult.class);
                    if (restResult.getCode()== RestConsts.DEFAULT_SUCCESS_CODE){
                        return true;
                    }else return false;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        });

        boolean flag = write(fileData, fileSystem, path);//path="/a/iverson/love.jpg"

        if(flag&&completableFuture1.join()&&completableFuture2.join()){

//            //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!交给ms处理元数据更新
//            //1.获取ZK中metaserver可用节点的节点值。
//            String jsonStr = zkUtil.getMetaServerData();
//            if (jsonStr==null){
//                //表示两台metaserver都挂了。
//                return false;
//            }
//            //2.把metaserver节点值是json字符串，转化为对象
//            MetaServerInfo metaServerInfo = JacksonUtil.toBean(jsonStr, MetaServerInfo.class);
//            //3.拼接url，访问metaserver的地址
//            String url="http://"+metaServerInfo.getHost()+":"+metaServerInfo.getPort()+"/write?path="+path+"&length="+length;
//            String jsonR=null;
//            try {
//                String fileAndMetaDataJson = JacksonUtil.toJsonStr(fileAndMetaData);
//                jsonR = HttpClientUtils.postParameters(url,fileAndMetaDataJson,"application/json");
//            } catch (Exception e) {throw new RuntimeException(e);}
//            //4.获取httpclient请求的结果，转化为对象
//            RestResult restResult = JacksonUtil.toBean(jsonR, RestResult.class);
//            if (restResult.getCode()==RestConsts.DEFAULT_SUCCESS_CODE){
//                return true;//三副本写完，并且强一致性更新元数据。成功才返回true
//            }else return false;
            return true;

        }
        else return false;


    }
    public boolean write(byte[] data, String fileSystem, String path) {
        // 写到本地
        File file = new File(fileSystem + "/" + environment.getProperty("spring.application.name") + path);
        try (FileOutputStream fileOutputStream = new FileOutputStream(file, true)) { // try-with-resources 结构
            fileOutputStream.write(data);
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            return false;
        }
        // 如果没有错误，就返回 true
        return true;
    }

    //内存中维护一份map，保存上次链接的inputStream，不然每次执行此方法都只能从头开始读
    public ByteAndIntVo read(String path, int offset, int length) {
        FileInputStream inputStream = fileStreams.get(path);

        if (inputStream == null) {
            File file = new File(path);
            try {
                inputStream = new FileInputStream(file);
                fileStreams.put(path, inputStream);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        byte[] bytes = new byte[length];
        int point;
        try {
            point = inputStream.read(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (point == -1) {
            // 文件已经读取完毕
            fileStreams.remove(path);
            try {
                inputStream.close();
            } catch (IOException e) {
                // Handle error
            }
        }

        ByteAndIntVo byteAndIntVo = new ByteAndIntVo(bytes, point);
        return byteAndIntVo;
    }
    //客户端调用关闭流close方法时候执行此方法
    public void deleteMap(String path) {
        fileStreams.remove(path);//优雅的关闭流
    }


    public boolean mkdir(String fileSystem,String path, List<DataServerInfo> dataServerInfos) {
        //1.异步分发给另外两个节点创建
        String url1="http://"+dataServerInfos.get(0).getHost()+":"+dataServerInfos.get(0).getPort()+"/mkdir?path="+path;
        String url2="http://"+dataServerInfos.get(1).getHost()+":"+dataServerInfos.get(1).getPort()+"/mkdir?path="+path;
        //第一个异步线程启动
        CompletableFuture<Boolean> completableFuture1 = CompletableFuture.supplyAsync(new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                try {
                    String jsonR = HttpClientUtils.getAndSetHeader(url1,"fileSystem",fileSystem);
                    RestResult restResult = JacksonUtil.toBean(jsonR, RestResult.class);
                    if (restResult.getCode()== RestConsts.DEFAULT_SUCCESS_CODE){
                        return true;
                    }else return false;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        });

        //第二个异步线程启动
        CompletableFuture<Boolean> completableFuture2 = CompletableFuture.supplyAsync(new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                try {
                    String jsonR = HttpClientUtils.getAndSetHeader(url2,"fileSystem",fileSystem);
                    RestResult restResult = JacksonUtil.toBean(jsonR, RestResult.class);
                    if (restResult.getCode()== RestConsts.DEFAULT_SUCCESS_CODE){
                        return true;
                    }else return false;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        });


        //2.本地创建文件夹
        boolean flag = mkdir(fileSystem,path);

        // join()方法就是获取CompletableFuture的返回值。会一直等待直到取到结果
        //三个副本都创建成功才返回true
        if (flag&&completableFuture1.join()&&completableFuture2.join()){
            return true;
        }else return false;


    }
    public boolean mkdir(String fileSystem,String path) {//fileSystem="D:/MyIDE/JavaProjects/minfs_student/dataServerDiskSpace"
        //1.本地创建文件夹
        //String prefix = environment.getProperty("storage-address")+environment.getProperty("spring.application.name");
        File file=new File(fileSystem+"/"+environment.getProperty("spring.application.name")+path);
        return file.mkdirs();
    }


    public boolean delete(String fileSystem, String path) {
        //判断当前文件或者文件夹是否存在：
        String absolutePath=fileSystem+"/"+environment.getProperty("spring.application.name")+path;
        File file=new File(absolutePath);
        boolean exists = file.exists();
        if (!exists){//文件不存在就不用删除了
            return true;
        }
        //文件或者文件夹存在，需要删除磁盘文件,遍历该文件夹下所有文件，得到总大小
        FileStats stats = calculateFileStats(absolutePath);
        long totalSize = stats.totalSize;//所有目录下文件总大小
        int fileCount = stats.fileCount;//当前文件夹下所有文件数量

        //修改zk上ds的大小,异步执行
        CompletableFuture<Boolean> completableFuture1 = CompletableFuture.supplyAsync(()->{
            CuratorFramework cf = zkUtil.getCuratorFramework();
            String dsPath=environment.getProperty("dataServer-prefix")+"/"+environment.getProperty("server.ip")+":"+environment.getProperty("server.port");
            try {
                byte[] bytes = cf.getData().forPath(dsPath);
                String json = new String(bytes);
                DataServerInfo dataServerInfo = JacksonUtil.toBean(json, DataServerInfo.class);
                dataServerInfo.setFileTotal(dataServerInfo.getFileTotal()-fileCount);
                dataServerInfo.setUseCapacity(dataServerInfo.getUseCapacity()-totalSize);
                String jsonNew = JacksonUtil.toJsonStr(dataServerInfo);
                cf.setData().forPath(dsPath,jsonNew.getBytes());
            } catch (Exception e) {throw new RuntimeException(e);}
            return true;
        });


        //删除物理磁盘
        boolean delete = deleteDirectory(file);


        //两个都完成返回true，否则返回false
        if (delete&&completableFuture1.join()){
            return true;
        }else return false;


    }
    public FileStats calculateFileStats(String path) {
        File file = new File(path);
        if (file.isFile()) {
            return new FileStats(file.length(), 1); // 文件的大小和数量
        }
        long totalSize = 0;
        int fileCount = 0;
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    FileStats childStats = calculateFileStats(child.getAbsolutePath());
                    totalSize += childStats.totalSize;
                    fileCount += childStats.fileCount;
                }
            }
        }
        return new FileStats(totalSize, fileCount);
    }

    public  boolean deleteDirectory(File directory) {
        if (directory.isDirectory()) {
            File[] children = directory.listFiles();
            if (children != null) {
                for (File child : children) {
                    boolean success = deleteDirectory(child);
                    if (!success) {
                        return false;
                    }
                }
            }
        }
        // 删除空目录或文件
        return directory.delete();
    }

    //把当前机器path路径文件写道另外的副本中。
    public void repairToOtherDS(String path, List<String> needRepairDS) throws IOException {//path=/a/b/c/four.jpg
        for (String hostAndPort :needRepairDS) {
            String url="http://"+hostAndPort+"/write?path="+path;
            String fileSystem=environment.getProperty("fileSystem");//fileSystem=D:/MyIDE/JavaProjects/minfs_student/dataServerDiskSpace

            File file=new File(environment.getProperty("fileSystem")+"/"+environment.getProperty("spring.application.name")+path);
            //确保文件夹存在,先去创建一个文件夹
            String parentPath = path.substring(0, path.lastIndexOf('/')); // 这将返回"/a/b/c"
            try {
                HttpClientUtils.getAndSetHeader("http://"+hostAndPort+"/mkdir?path="+parentPath,"fileSystem",fileSystem);
            } catch (Exception e) {throw new RuntimeException(e);}


            FileInputStream fis = new FileInputStream(file);
            //todo 这里设置恢复数据的时候缓冲区大小
            int defaultBufferSize=8388608;//默认8MB
            String bufferSize = environment.getProperty("defaultBufferSize");
            if (bufferSize!=null){
                defaultBufferSize= Integer.parseInt(bufferSize);
            }
            byte[] buffer=new byte[defaultBufferSize];
            int len = fis.read(buffer);
            while(len!=-1){
                if (len!=buffer.length){
                    //最后一次发送，可能长度不和buffer一致，只发生一部分数据。
                    //写到分布式系统中。
                    byte[] newBuffer=new byte[len];
                    System.arraycopy(buffer, 0, newBuffer, 0, len); // 更高效的数组复制
                    String jsonR = HttpClientUtils.postDataAndSetHeader(url,newBuffer,"fileSystem",fileSystem);
                    //RestResult restResult = JacksonUtil.toBean(jsonR, RestResult.class);

                    //fsOutputStream.write(buffer,0,len);//这个是我自己设计的分布式写方法
                    len=fis.read(buffer);
                }else {
                    //写到分布式系统中。
                    String jsonR = HttpClientUtils.postDataAndSetHeader(url,buffer,"fileSystem",fileSystem);
                    //RestResult restResult = JacksonUtil.toBean(jsonR, RestResult.class);

                    //fsOutputStream.write(buffer,0,len);//这个是我自己设计的分布式写方法
                    len=fis.read(buffer);
                }

            }
            fis.close();
        }
    }
}

