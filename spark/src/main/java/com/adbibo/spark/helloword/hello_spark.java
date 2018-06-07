package com.adbibo.java;

/**
 * Created by adbibo on 16/6/21.
 */


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import scala.Tuple2;
//import scala.actors.threadpool.Arrays;
import java.util.Arrays;

public class hello_spark {

    public static void main(String[] args){

        SparkConf conf=new SparkConf().setAppName("hello_spark").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("data/README.md");

        JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
        });

        System.out.println(words.toString());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });
//        counts.saveAsTextFile("hdfs://...");
        System.out.println("test--" + counts);
    }
}
