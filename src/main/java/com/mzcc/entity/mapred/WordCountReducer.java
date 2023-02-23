package com.mzcc.entity.mapred;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    Log log = LogFactory.get();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        /*其实直接get size 也可以*/
        for (IntWritable value : values) {
            sum += value.get();
        }
        log.info("当前key {}, 总数 " + "" + ": {}", key, sum);
        IntWritable total = new IntWritable(sum);
        context.write(key, total);
    }
}
