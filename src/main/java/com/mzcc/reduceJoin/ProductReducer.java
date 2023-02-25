package com.mzcc.reduceJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author wangChong
 */
public class ProductReducer extends Reducer<Text, ProductJoin, String, ProductJoin> {

    public ProductReducer() {
        super();
    }

    @Override
    protected void setup(Reducer<Text, ProductJoin, String, ProductJoin>.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<ProductJoin> values, Reducer<Text, ProductJoin, String, ProductJoin>.Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }

    @Override
    protected void cleanup(Reducer<Text, ProductJoin, String, ProductJoin>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void run(Reducer<Text, ProductJoin, String, ProductJoin>.Context context) throws IOException, InterruptedException {
        super.run(context);
    }
}
