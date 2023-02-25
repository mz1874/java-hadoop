package com.mzcc.output;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author 23391
 */
public class UrlRecordWriter extends RecordWriter<Text, NullWritable> {


    private  FSDataOutputStream firstOne;
    private  FSDataOutputStream firstTwo;

    public UrlRecordWriter(TaskAttemptContext conf){
        try {
            FileSystem system = FileSystem.get(conf.getConfiguration());
            firstOne = system.create(new Path("D:\\1.log"));
            firstTwo = system.create(new Path("D:\\2.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if (key.toString().contains("atguigu")) {
            firstOne.writeBytes(key.toString() + "\n");
        }else {
            firstTwo.writeBytes(key.toString() + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.close(firstTwo);
        IOUtils.close(firstOne);
    }
}
