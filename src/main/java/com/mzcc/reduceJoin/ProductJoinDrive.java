package com.mzcc.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ProductJoinDrive {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(ProductJoinDrive.class);

        job.setMapperClass(ProductMap.class);
        job.setReducerClass(ProductReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ProductJoin.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ProductJoin.class);

        FileInputFormat.setInputPaths(job,
                "D:\\input");
        FileOutputFormat.setOutputPath(job, new Path("D:\\333"));

        boolean b = job.waitForCompletion(true);

    }
}
