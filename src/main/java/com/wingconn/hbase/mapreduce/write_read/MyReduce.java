package com.wingconn.hbase.mapreduce.write_read;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by albert.shen on 2018/7/3.
 */
public class MyReduce extends TableReducer<Text, IntWritable,
        ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (IntWritable val : values) {
            i += val.get();
        }
        Put put = new Put(Bytes.toBytes(key.toString()));
        put.addColumn(Bytes.toBytes("column"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf (i)));
        System.out.println(key.toString() + "\t" + i);
        context.write(null, put);
    }
}
