package com.mzcc.mapred;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text out = new Text();
    private final IntWritable intWritable = new IntWritable(1);


    /**
     * @param key     偏移量
     * @param value   value 每行
     * @param context 上下文通信
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String valueString = value.toString();
        String[] arr = StringUtils.split(valueString, " ");
        for (String i : arr) {
            out.set(i);
            context.write(out, intWritable);
        }
    }
}
