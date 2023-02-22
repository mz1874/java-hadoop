package com.mzcc.entity.dataflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * @author aiden
 * @data 19/02/2023
 * @description
 */
public class DataFlowMap extends Mapper<LongWritable, Text, Text, DataFlowEntity> {

    Text text;
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DataFlowEntity>.Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String[] arr = string.split(" ");
        DataFlowEntity dataFlowEntity = new DataFlowEntity();
        dataFlowEntity.setUpFlowNumber(Integer.valueOf(arr[1]));
        dataFlowEntity.setDownFlowNumber(Integer.valueOf(arr[2]));
        dataFlowEntity.setTotalDateFlowNumber();
        text = new Text(arr[0]);
        context.write(text, dataFlowEntity);
    }
}
