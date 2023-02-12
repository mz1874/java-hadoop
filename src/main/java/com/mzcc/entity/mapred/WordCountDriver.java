package com.mzcc.entity.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        Configuration configuration = new Configuration();
//
        Job instance = Job.getInstance();
//
//        instance.setJarByClass(WordCountDriver.class);
//
//
//        instance.setMapperClass(WordCountMap.class);
//        instance.setReducerClass(WordCountReducer.class);
//
//        instance.setMapOutputKeyClass(Text.class);
//        instance.setMapOutputValueClass(IntWritable.class);
//
//        instance.setOutputKeyClass(Text.class);
//        instance.setOutputValueClass(IntWritable.class);
//
//        // 6 设置输入和输出路径
//        FileInputFormat.setInputPaths(instance, "C:\\Users\\23391\\Desktop\\1.txt");
//        FileOutputFormat.setOutputPath(instance, new Path("C:\\Users\\23391\\Desktop"));
//
//        // 7 提交 job
//        boolean result = instance.waitForCompletion(true);
//        System.exit(result ? 0 : 1);

    }
}
