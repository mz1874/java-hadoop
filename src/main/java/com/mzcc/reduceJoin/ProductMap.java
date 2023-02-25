package com.mzcc.reduceJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ProductMap extends Mapper<Long, Text, Text, ProductJoin> {

    private String name;
    Text text = new Text();

    public ProductMap() {
        super();
        System.out.printf("构造器");
    }

    @Override
    protected void setup(Mapper<Long, Text, Text, ProductJoin>.Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }


    @Override
    protected void map(Long key, Text value, Mapper<Long, Text, Text, ProductJoin>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        ProductJoin productJoin = new ProductJoin();
        if (this.name.contains("order")) {
            productJoin.setId(split[0]);
            productJoin.setPid(split[1]);
            productJoin.setQuantity(split[2]);
            productJoin.setFlag("order");
        }else{
            productJoin.setId(split[0]);
            productJoin.setPid(split[1]);
            productJoin.setName(split[2]);
            productJoin.setFlag("product");
        }
        text.set(productJoin.getPid());
        context.write(text, productJoin);
        System.out.println("Map");
    }

    @Override
    protected void cleanup(Mapper<Long, Text, Text, ProductJoin>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        System.out.println("cleanUp");
    }

    @Override
    public void run(Mapper<Long, Text, Text, ProductJoin>.Context context) throws IOException, InterruptedException {
        super.run(context);
        System.out.println("RUN");
    }
}
