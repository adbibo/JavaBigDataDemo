package com.adbibo.spark.hadoop;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by adbibo on 2017/2/15.
 */
public class ConnectHadoop {

    public static final String master = "spark://namenode2:7077";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        System.out.println(sc);
        sc.stop();
    }
}
