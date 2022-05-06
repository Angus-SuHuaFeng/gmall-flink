package com.angus.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author ：Angus
 * @date ：Created in 2022/2/21 15:36
 * @description： 接收模拟器生成的数据，并对数据进行处理
 */
/*
    @Controller : 将对象的创建交给Spring容器 , 方法返回String的话会默认当作页面跳转处理
    @RestController :  =>  @Controller + @ResponseBody  方法返回Object, 会转换为json格式字符串进行响应
 */
@RestController
@Slf4j
public class LoggerController {
//    Spring 对kafka的支持
    // @Autowired 将KafkaTemplate注入到Controller中
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    // 处理 http://localhost:8080/applog

    // 提供一个方法,处理模拟器生成的数据
    // @RequestMapping("/applog) 把applog请求交给方法进行处理
    // @RequestBody : 表示从请求体中获取数据
    // @ResponseBody : 如果类用的是Controller,而类中的方法不想做页面跳转时,需要加此注解

    @RequestMapping("/applog")
    public String appLog(@RequestParam("param") String jsonStr){
        // 使用日志实现落盘
        log.info(jsonStr);
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }

}
