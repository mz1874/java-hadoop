package com.mzcc.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 * @author 23391
 */
public class WordCountTool implements Tool {

    Configuration configuration;

    @Override
    public int run(String[] strings) throws Exception {
        Job job = new Job(configuration);
        
        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.configuration;
    }
}
