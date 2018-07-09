package com.wingconn.hbase.mapreduce.write_read;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by albert.shen on 2018/7/3.
 */
public class MySummaryJob {
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create ( );
        String family="cf";
        String column="attr1";
        config.set ("hbase.zookeeper.quorum.", "node-1,node-2,node-3");
        Job job = Job.getInstance (config, "ExampleSummary");
        job.setJarByClass (MySummaryJob.class);
        Scan scan = new Scan ( );
        scan.setCaching (500);
        scan.setCacheBlocks (false);
        scan.addColumn (Bytes.toBytes (family), Bytes.toBytes (column));
        TableMapReduceUtil.initTableMapperJob ("word", scan, MyMapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob ("wordcount", MyReduce.class, job);
        job.setNumReduceTasks (1);
        boolean b = job.waitForCompletion (true);
        if (!b) {
            throw new IOException ("error with job");
        }
    }
}
