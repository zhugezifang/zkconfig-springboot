package com.vince.xq.zkconfig.config;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

@Configuration
public class ZookeeperConfig {

    Properties properties = new Properties();
    //可以理解curatorFramework为客户端，基本操作都由它完成
    CuratorFramework curatorFramework = null;
    TreeCache treeCache = null;

    @Value("${zookeeper.url}")
    private String zkUrl;
    //定义顶级
    private final String CONFIG_NAME = "/zookeeper";

    //初始化
    private void init() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        curatorFramework = CuratorFrameworkFactory.newClient(zkUrl, retryPolicy);
        treeCache = new TreeCache(curatorFramework, CONFIG_NAME);
    }

    /**
     * 设置属性
     * @param key
     * @param value
     * @throws Exception
     */
    public void setProperties(String key, String value) throws Exception {
        String propertiesKey = CONFIG_NAME + "/" + key;
        Stat stat = curatorFramework.checkExists().forPath(propertiesKey);
        if(stat == null) {
            curatorFramework.create().forPath(propertiesKey);
        }
        curatorFramework.setData().forPath(propertiesKey, value.getBytes());
    }

    /**
     * 获取属性
     * @param key
     * @return
     */
    public String getProperties(String key) {
        return properties.getProperty(key);
    }

    @PostConstruct
    public void loadProperties() {
        try {
            init();
            curatorFramework.start();
            treeCache.start();

            // 从zk中获取配置放入本地配置中
            Stat stat = curatorFramework.checkExists().forPath(CONFIG_NAME);
            if(stat == null) {
                curatorFramework.create().forPath(CONFIG_NAME);
            }
            List<String> configList = curatorFramework.getChildren().forPath(CONFIG_NAME);
            for (String configName : configList) {
                byte[] value = curatorFramework.getData().forPath(CONFIG_NAME + "/" + configName);
                properties.setProperty(configName, new String(value));
            }

            // 监听属性值变更
            treeCache.getListenable().addListener(new TreeCacheListener() {
                @Override
                public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                    if (Objects.equals(treeCacheEvent.getType(), TreeCacheEvent.Type.NODE_ADDED) ||
                            Objects.equals(treeCacheEvent.getType(), TreeCacheEvent.Type.NODE_UPDATED)) {
                        String updateKey = treeCacheEvent.getData().getPath().replace(CONFIG_NAME + "/", "");
                        properties.setProperty(updateKey, new String(treeCacheEvent.getData().getData()));
                        System.out.println("数据更新: "+treeCacheEvent.getType()+", key:"+updateKey+",value:"+new String(treeCacheEvent.getData().getData()));
                    }
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

