package com.mzcc.reduceJoin;

import cn.hutool.core.bean.BeanUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        List<ProductJoin> list = new ArrayList<>();
        ProductJoin product = new ProductJoin();
        String pid = key.toString();
        values.forEach(e -> {
            if ("product".equals(e.getFlag())) {
                BeanUtil.copyProperties(e, product);
            } else {
                /**
                 *   productJoin.setId(split[0]);
                 *   productJoin.setPid(split[1]);
                 *   productJoin.setQuantity(split[2]);
                 *   productJoin.setFlag("order");
                 */
                ProductJoin productJoin = new ProductJoin();
                BeanUtil.copyProperties(e, productJoin);
                list.add(productJoin);
            }
        });
        for (ProductJoin productJoin : list) {
            productJoin.setName(product.getName());
            context.write(pid, productJoin);
        }


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
