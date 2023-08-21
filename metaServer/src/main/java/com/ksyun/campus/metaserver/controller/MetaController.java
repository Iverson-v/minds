package com.ksyun.campus.metaserver.controller;

import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.entity.DataServerInfo;
import com.ksyun.campus.metaserver.entity.FileAndMetaData;
import com.ksyun.campus.metaserver.entity.RestResult;
import com.ksyun.campus.metaserver.services.MetaService;
import com.ksyun.campus.metaserver.util.ZkUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;


@RestController("/")
@Api(tags="MetaServer")
public class MetaController {

    @Autowired
    private MetaService metaService;


    @RequestMapping("stats")
    @ApiOperation("根据文件路径（或者文件夹）返回文件的状态信息")
    public StatInfo stats(@RequestParam String path){
        StatInfo statInfo=metaService.getFileStats(path);
        return statInfo;
    }
    @RequestMapping("create")
    @ApiOperation("MetaServer处理客户端写文件请求")
    public List<DataServerInfo> createFile(@RequestHeader String fileSystem, @RequestParam String path){
        List<DataServerInfo> dataServerInfos = metaService.pickDataServer();

        return dataServerInfos;//告诉client哪三个ds可以写。
    }
    @RequestMapping("mkdir")
    @ApiOperation("MetaServer处理客户端创建文件夹请求")
    public RestResult mkdir(@RequestHeader String fileSystem,@RequestParam String path){

        boolean flag=metaService.mkdir(fileSystem,path);
        if (flag){
            return RestResult.success();
        }else return  RestResult.failure();
    }
    @RequestMapping("listdir")
    @ApiOperation("MetaServer处理客户端列出文件列表请求")
    public List<StatInfo> listdir(@RequestParam String path){
        List<StatInfo> list=metaService.listFileStats(path);
        return list;
    }
    @RequestMapping("delete")
    @ApiOperation("MetaServer处理客户端删除文件夹请求")
    public RestResult delete(@RequestHeader String fileSystem, @RequestParam String path){
        boolean flag=metaService.delete(fileSystem,path);
        if (flag){
            return RestResult.success();
        }else return RestResult.failure();

    }

    /**
     * 保存文件写入成功后的元数据信息，包括文件path、size、三副本信息等
     */
    @RequestMapping("write")
    @ApiOperation("MetaServer处理ds的更新元数据请求")
    public RestResult commitWrite(@RequestParam String path, @RequestBody List<DataServerInfo> dataServerInfos, @RequestParam int length){
        boolean flag=metaService.commitWrite(path,dataServerInfos,length);
        if (flag){
            return RestResult.success();
        }else return RestResult.failure();
    }

    /**
     * 根据文件path查询三副本的位置，返回客户端具体ds、文件分块信息
     * @param fileSystem
     * @param path
     * @return
     */
    @RequestMapping("open")
    @ApiOperation("MetaServer处理客户端读请求")
    public ResponseEntity open(@RequestHeader String fileSystem,@RequestParam String path){
       // StatInfo statInfo=metaService.selectOneDataServer(fileSystem,path);
        return new ResponseEntity(HttpStatus.OK);
    }

    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer(){
        System.exit(-1);
    }

}
