package com.wingconn.hbase.mapreduce.write_read;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by albert.shen on 2018/7/3.
 */
public class MyMapper extends TableMapper<Text, IntWritable> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
      //  Byte[] word= value.getValue ("cf","attr1");
        try{
          for(Cell cell:value.listCells ()){
              String qualifier = new String(CellUtil.cloneQualifier(cell));
              String colValue = new String(CellUtil.cloneValue(cell), "UTF-8");
              System.out.print(qualifier + "=" + colValue + "\t");
              context.write(new Text(colValue), new IntWritable(1));
          }
      }catch (Exception e){
          e.printStackTrace ();
      }
    }
}
