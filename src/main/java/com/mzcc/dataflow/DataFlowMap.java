package com.mzcc.dataflow;

import com.mzcc.entity.DataFlowEntity;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
        String[] arr = string.split("\t");
        DataFlowEntity dataFlowEntity = new DataFlowEntity();
        String arr4 = arr[4];
        String arr5 = arr[5];
        if ("".equals(arr4)){
            arr4 = "0";
        }
        if ("".equals(arr5)){
            arr5 = "0";
        }
        dataFlowEntity.setUpFlowNumber(Integer.valueOf(arr4));
        dataFlowEntity.setDownFlowNumber(Integer.valueOf(arr5));
        dataFlowEntity.setTotalDateFlowNumber();
        text = new Text(arr[1]);
        context.write(text, dataFlowEntity);
    }
}
