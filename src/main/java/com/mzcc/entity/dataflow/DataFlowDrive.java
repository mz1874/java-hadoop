package com.mzcc.entity.dataflow;

import com.mzcc.entity.mapred.WordCountDriver;
import com.mzcc.entity.mapred.WordCountMap;
import com.mzcc.entity.mapred.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * @author aiden
 * @data 22/02/2023
 * @description
 */
public class DataFlowDrive {

    /**
     * 输出路径
     */
    public static String OUT_PUT_FOLDER;

    /**
     * 输入路径
     */
    public static String INPUT_FILE_NAME;


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        setEnv();

        Configuration configuration = new Configuration();
        Job instance = Job.getInstance(configuration);
        instance.setJarByClass(DataFlowDrive.class);

        instance.setMapperClass(DataFlowMap.class);
        instance.setReducerClass(DataFlowReducer.class);

        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(DataFlowEntity.class);

        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(DataFlowEntity.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(instance, INPUT_FILE_NAME);

        FileOutputFormat.setOutputPath(instance, new Path(OUT_PUT_FOLDER));

        // 7 提交 job
        boolean result = instance.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

    public static void setEnv() {
        String osName = System.getProperty("os.name");
        if ("Mac OS X".equals(osName)){
            OUT_PUT_FOLDER = "/Users/aiden/Downloads/1";
            INPUT_FILE_NAME = "/Users/aiden/Downloads/tel.txt";
        }else {
            OUT_PUT_FOLDER = "C:\\1";
        }

        /**
         * 提前删除输出路径
         */
        File file = new File(OUT_PUT_FOLDER);
        if (file.exists()) {
            File[] files = file.listFiles();
            for (File file1 : files) {
                file1.delete();
            }
            file.delete();
        }
    }
}
