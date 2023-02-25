package com.mzcc.dataflow;

import com.mzcc.entity.DataFlowEntity;
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

    @Override
    protected void reduce(Text key, Iterable<DataFlowEntity> values, Reducer<Text, DataFlowEntity, Text, Text>.Context context) throws IOException, InterruptedException {
        values.forEach(e -> {
            sumUp += Integer.valueOf(e.getUpFlowNumber());
            sumDown += Integer.valueOf(e.getDownFlowNumber());
        });
        Text text = new Text();
        text.set(sumUp + "\t" + sumDown + "\t" + (sumUp + sumDown));
        context.write(key, text);


    }
}
