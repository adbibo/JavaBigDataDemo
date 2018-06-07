package com.adbibo.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by adbibo on 2017/2/16.
 */
public class WordMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        String line = value.toString();
        for(String word : line.split("\\W+")){
            if(word.length()>0){
                output.collect(new Text(word),new IntWritable(1));
            }
        }
    }
}