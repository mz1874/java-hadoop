package com.mzcc.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;


public class WordCountDriver {

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
        FileInputFormat.setInputPaths(instance, "/Users/aiden/Downloads/Phineas Finn.txt");

        FileOutputFormat.setOutputPath(instance, new Path(OUT_PUT_FOLDER));

        // 7 提交 job
        boolean result = instance.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

    public static void setEnv() {
        String osName = System.getProperty("os.name");
        if ("Mac OS X".equals(osName)){
            OUT_PUT_FOLDER = "/Users/aiden/Downloads/1";
            INPUT_FILE_NAME = "/Users/aiden/Downloads/Phineas Finn.txt";
        }else {
            OUT_PUT_FOLDER = "C:\\1";
        }
    }
}
