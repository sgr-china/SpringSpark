package com.sgr.conf;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author sunguorui
 * @date 2021年12月02日 10:40 上午
 */
@Configuration
public class SparkConfig {
    @Value("${spark.app.name}")
    private String appName;
    @Value("${spark.master.uri}")
    private String sparkMasterUri;
    @Value("${spark.driver.memory}")
    private String sparkDriverMemory;
    @Value("${spark.executor.memory}")
    private String sparkExecutorMemory;
    @Value("${spark.executor.cores}")
    private String sparkExecutorCores;
    @Value("${spark.num.executors}")
    private String sparkExecutorsNum;
    @Bean
    @ConditionalOnMissingBean(SparkSession.class)
    public SparkSession sparkSession() {
        SparkConf sparkConf = new SparkConf().setAppName(appName)
                .setMaster(sparkMasterUri)
                .set("spark.driver.memory",sparkDriverMemory)
                .set("spark.executor.memory",sparkExecutorMemory)
                .set("spark.executor.cores",sparkExecutorCores)
                .set("spark.num.executors",sparkExecutorsNum);
        SparkSession spark = new SparkSession.Builder().enableHiveSupport()
                .config(sparkConf)
                .config("spark.debug.maxToStringFields", 1000)
                .getOrCreate();
        return spark;
    }
}
