package com.mzcc.entity.dataflow;

import com.mzcc.entity.dataflow.DataFlowEntity;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, DataFlowEntity> {

    @Override
    public int getPartition(Text text, DataFlowEntity dataFlowEntity, int numPartitions) {
        String tel = text.toString();
        String substring = tel.substring(0, 3);

        if (substring.equals("137")){
            return 1;
        }else if (substring.equals("135")){
            return 2;
        }else {
            return 0;
        }
    }
}
