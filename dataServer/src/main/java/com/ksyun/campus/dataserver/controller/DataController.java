package com.ksyun.campus.dataserver.controller;

import com.ksyun.campus.dataserver.entity.ByteAndIntVo;
import com.ksyun.campus.dataserver.entity.DataServerInfo;
import com.ksyun.campus.dataserver.entity.FileAndMetaData;
import com.ksyun.campus.dataserver.entity.RestResult;
import com.ksyun.campus.dataserver.services.DataService;
import com.ksyun.campus.dataserver.util.jaksonutils.JacksonUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

@RestController("/")
@Api(tags="DataServer")
public class DataController {

    @Autowired
    private Environment environment;

    @Autowired
    private DataService dataService;
    /**
     * 1、读取request content内容并保存在本地磁盘下的文件内
     * 2、同步调用其他ds服务的write，完成另外2副本的写入
     * 3、返回写成功的结果及三副本的位置
     * @param fileSystem
     * @param path
//     * @param offset
//     * @param length
     * @return
     */
    //public ResponseEntity writeFile(@RequestHeader String fileSystem, @RequestParam String path, @RequestParam int offset, @RequestParam int length)
    @RequestMapping("writeMaster")
    public void writeFileMaster(@RequestBody FileAndMetaData fileAndMetaData, @RequestHeader String fileSystem, @RequestParam String path){
        boolean flag = dataService.write(fileAndMetaData, fileSystem, path);
        if (flag){

        }
    }
    @RequestMapping("write")
    public RestResult writeFile(@RequestBody byte[] data,@RequestHeader String fileSystem, @RequestParam String path){
        boolean flag= dataService.write(data, fileSystem, path);
        if (flag){
            return RestResult.success();
        }else return RestResult.failure();
    }

    /**
     * 在指定本地磁盘路径下，读取指定大小的内容后返回
     * @param fileSystem
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping("read")
    @ApiOperation("读取文件方法")
    public ByteAndIntVo readFile(@RequestHeader String fileSystem, @RequestParam String path, @RequestParam int offset, @RequestParam int length){
        path=fileSystem+"/"+environment.getProperty("spring.application.name")+path;
        ByteAndIntVo byteAndIntVo = dataService.read(path, offset, length);

        return byteAndIntVo;
    }
    @RequestMapping("closeReadStream")
    @ApiOperation("客户端调用关闭流close方法时候执行此方法")
    public void closeReadStream(@RequestHeader String fileSystem, @RequestParam String path){
        path=fileSystem+"/"+environment.getProperty("spring.application.name")+path;
        dataService.deleteMap(path);

    }



    //创建文件夹，并且完成另外2副本的创建,只能接受post请求。
    @PostMapping("mkdirMaster")
    @ApiOperation("主副本创建文件夹接口，只接受post请求")
    public RestResult mkdir(@RequestHeader String fileSystem,@RequestBody List<DataServerInfo> dataServerInfos,@RequestParam String path){
        boolean flag=dataService.mkdir(fileSystem,path,dataServerInfos);
        if (flag){
            return RestResult.success();
        }else return RestResult.failure();
    }
    @RequestMapping("mkdir")
    @ApiOperation("非主副本创建文件夹接口")
    public RestResult mkdir(@RequestHeader String fileSystem,@RequestParam String path){
        boolean flag=dataService.mkdir(fileSystem,path);
        if (flag){
            //如果创建成功，则需要
            return RestResult.success();
        }else return RestResult.failure();
    }


    @RequestMapping("delete")
    @ApiOperation("删除接口")
    public RestResult delete(@RequestHeader String fileSystem,@RequestParam String path){
        boolean flag=dataService.delete(fileSystem,path);

        if (flag){
            //如果创建成功，则需要
            return RestResult.success();
        }else return RestResult.failure();
    }




    @RequestMapping("checkReplicaCount")
    @ApiOperation("根据路径检查副本是否存在")
    public RestResult checkReplicaCount(@RequestParam String path){
        path=environment.getProperty("fileSystem")+"/"+environment.getProperty("spring.application.name")+path;
        File file=new File(path);
        boolean flag = file.exists();
        if (flag){
            return RestResult.success();
        }else return RestResult.failure();
    }

    @RequestMapping("repair")
    @ApiOperation("接受定时任务的修复文件接口")
    public void repair(@RequestParam String path,@RequestBody String needRepairDSJsonStr){
        //path=environment.getProperty("fileSystem")+"/"+environment.getProperty("spring.application.name")+path;
        List<String> needRepairDS = JacksonUtil.toBeanList(needRepairDSJsonStr, String.class);//list里面装的是要发送修复的host:port机器。

        try {
            dataService.repairToOtherDS(path,needRepairDS);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //todo
    }







    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer(){
        System.exit(-1);
    }
}
