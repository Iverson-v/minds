package com.ksyun.campus.dataserver.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

/**
 * 服务启动运行逻辑
 */
@Component
public class ServiceAppRunner implements ApplicationRunner {

    @Autowired
    private RegistService registService;
    @Override
    public void run(ApplicationArguments args) throws Exception {

        // 此处代码会在 Boot 应用启动时执行

        // 1. 向 zk 服务注册当前服务
        registService.registToCenter();



        // 2. 如果网络波动之后连接上重新注册
        registService.reRegist();



    }

}
