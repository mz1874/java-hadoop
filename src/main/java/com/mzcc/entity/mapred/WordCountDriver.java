package com.mzcc.entity.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;

public class WordCountDriver {

    public final static String OUT_PUT_FOLDER = "C:\\1";


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        File file = new File(OUT_PUT_FOLDER);
        if (file.exists()) {
            File[] files = file.listFiles();
            for (File file1 : files) {
                file1.delete();
            }
            file.delete();
        }

        Configuration configuration = new Configuration();
        Job instance = Job.getInstance(configuration);
        instance.setJarByClass(WordCountDriver.class);

        instance.setMapperClass(WordCountMap.class);
        instance.setReducerClass(WordCountReducer.class);

        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(IntWritable.class);

        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(instance, "D:\\2.txt");

        FileOutputFormat.setOutputPath(instance, new Path("C:\\1"));

        // 7 提交 job
        boolean result = instance.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
