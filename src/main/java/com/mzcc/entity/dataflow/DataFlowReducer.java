package com.mzcc.entity.dataflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author aiden
 * @data 22/02/2023
 * @description
 */
public class DataFlowReducer extends Reducer<Text, DataFlowEntity, Text, Text> {

    int sumUp = 0;
    int sumDown = 0;

    int index = 0;

    @Override
    protected void reduce(Text key, Iterable<DataFlowEntity> values, Reducer<Text, DataFlowEntity, Text, Text>.Context context) throws IOException, InterruptedException {
        values.forEach(e -> {
            sumUp += Integer.valueOf(e.getUpFlowNumber());
            sumDown += Integer.valueOf(e.getDownFlowNumber());
        });
        Text text = new Text();
        if (index == 0) {
            text.set(
                    "\n手机号\t上行流量\t下行流量\t总流量\n"+
                    sumUp + "\t" + sumDown + "\t" + (sumUp + sumDown)
            );
            index = -1;
        }else {
            text.set(sumUp + "\t" + sumDown + "\t" + (sumUp + sumDown));
        }
        context.write(key, text);


    }
}
