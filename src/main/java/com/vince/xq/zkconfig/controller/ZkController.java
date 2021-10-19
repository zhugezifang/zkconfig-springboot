package com.vince.xq.zkconfig.controller;

import com.vince.xq.zkconfig.config.ZookeeperConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ZkController {

    @Autowired
    private ZookeeperConfig zookeeperConfig;


    @GetMapping("get/{key}")
    public String getProperties(@PathVariable String key) {
        return zookeeperConfig.getProperties(key);
    }

    @GetMapping("set/{key}/{value}")
    public String setProperties(@PathVariable String key, @PathVariable String value) throws Exception {
        zookeeperConfig.setProperties(key, value);
        return "配置成功";
    }
}


