import com.google.protobuf.InvalidProtocolBufferException;
import com.wingconn.hbase.entity.CellEntity;
import com.wingconn.hbase.entity.ColumnFamilyEntity;
import com.wingconn.hbase.entity.HBasePageModel;
import com.wingconn.hbase.entity.QualiferEntity;
import com.wingconn.hbase.probuf.Person;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;
import org.junit.Test;
import com.wingconn.hbase.util.HBaseUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by albert.shen on 2018/5/29.
 */
public class HbaseApi {
    @Before
    public void init(){
        HBaseUtil.init ("node-1,node-2,node-3");
    }
    @Test
    public void createTable(){
        String[] columnFamliy=new String[]{"cf1"};
        HBaseUtil.createTable ("psn",columnFamliy);
    }

    @Test
    public void deleteTable(){
        HBaseUtil.deleteTable ("psn");
    }

    @Test
    public void getAllRow(){
     List<Cell[]> list=HBaseUtil.findAll ("phone");
     for(Cell[] cell:list){
       System.out.println("长度：" + cell.length);
                for (int i = 0; i < cell.length; i++) {
                    System.out.println("信息:"
                            + new String(CellUtil.cloneFamily(cell[i])) + " "
                            + new String(CellUtil.cloneQualifier(cell[i]))
                            + "  " + new String(CellUtil.cloneValue(cell[i]))
                            + " " + new String(CellUtil.cloneRow(cell[i])));
                }
         System.out.println("\n-----------------------");
       }
    }

    @Test
    public void findByRowKeys()  {
        List<String> list=new ArrayList<String> ();
        list.add ("13013861829");
        list.add ("18912711510");
        Result[] results=  HBaseUtil.findByRowKeys ("phone",list);
        for(Result result:results){
            Cell[] cell=  result.rawCells ();
            System.out.println("长度：" + cell.length);
            for (int i = 0; i < cell.length; i++) {
                System.out.println("信息:"
                        + new String(CellUtil.cloneFamily(cell[i])) + " "
                        + new String(CellUtil.cloneQualifier(cell[i]))
                        + "  " + new String(CellUtil.cloneValue(cell[i]))
                        + " " + new String(CellUtil.cloneRow(cell[i])));
            }
            System.out.println("\n-----------------------");
        }
    }

    @Test
    public void findByRowKey(){
        Result result= HBaseUtil.findByRowKey ("phone","13013861829");
        System.out.println ("tel="+new String(CellUtil.cloneValue (result.getColumnLatestCell ("cf1".getBytes (),"tel".getBytes ()))));
        System.out.println ("owner="+new String(CellUtil.cloneValue (result.getColumnLatestCell ("cf1".getBytes (),"owner".getBytes ()))));
    }

    @Test
    public void findByPage(){
        HBasePageModel pageModel = new HBasePageModel(10);
        pageModel =HBaseUtil.scanResultByPageFilter("phone",null,null,null,0,pageModel);
        for(Result result:pageModel.getResultList ()){
            Cell[] cell=  result.rawCells ();
            System.out.println("长度：" + cell.length);
            for (int i = 0; i < cell.length; i++) {
                System.out.println("信息:"
                        + new String(CellUtil.cloneFamily(cell[i])) + " "
                        + new String(CellUtil.cloneQualifier(cell[i]))
                        + "  " + new String(CellUtil.cloneValue(cell[i]))
                        + " " + new String(CellUtil.cloneRow(cell[i])));
            }
            System.out.println("\n-----------------------");
        }
    }

    @Test
    public void putData(){
        List<CellEntity> cells=new ArrayList<CellEntity> ();
        CellEntity cell=new CellEntity ();
        cell.setCellName ("tel");
        cell.setCellValue ("18912711510");
        cells.add (cell);
        CellEntity cell2=new CellEntity ();
        cell2.setCellName ("owner");
        cell2.setCellValue ("沈建林");
        cells.add(cell2);
        List<QualiferEntity> qualiferies=new ArrayList<QualiferEntity> ();
        QualiferEntity qualiferEntity=new QualiferEntity ();
        qualiferEntity.setQualiferName ("cf1");
        qualiferEntity.setQualiferValue (cells);
        qualiferies.add (qualiferEntity);
        ColumnFamilyEntity columnFamilyEntity=new ColumnFamilyEntity ();
        columnFamilyEntity.setQualifers (qualiferies);
        List<ColumnFamilyEntity> columnFamilyEntities=new ArrayList<ColumnFamilyEntity> ();
        columnFamilyEntities.add (columnFamilyEntity);
        HBaseUtil.puData ("phone","18912711510",columnFamilyEntities);
    }

    @Test
    public void savePersonByProbuf(){
        Person.User.Builder builer=Person.User.newBuilder ();
        builer.setAddress ("苏州");
        builer.setAge (12);
        builer.setUsername ("slj");
        builer.setEmail ("101@qq.com");
        Person.User user=builer.build ();
        Connection conn = HBaseUtil.getConnection ( );
        Put put = new Put ("0001".getBytes ( ));
        Table table=null;
        try {
              table = conn.getTable(TableName.valueOf("psn"));
              put.addColumn ("cf1".getBytes ( ), "info".getBytes (), user.toByteArray ());
              table.put (put);
        } catch (IOException e) {
            e.printStackTrace ( );
        }finally {
            if(table!=null){
                try {
                    table.close ();
                } catch (IOException e) {
                    e.printStackTrace ( );
                }
            }
            if(conn!=null){
                try {
                    conn.close ();
                } catch (IOException e) {
                    e.printStackTrace ( );
                }
            }
        }
    }
/*
    //模拟接收Byte[]，反序列化成Person类
    byte[] byteArray =person.toByteArray();
    Person p2 = Person.parseFrom(byteArray);
        System.out.println("after :" +p2.toString());*/

    @Test
    public void getByProbuf() throws InvalidProtocolBufferException {
        Result result= HBaseUtil.findByRowKey ("psn","0001");
        byte[] personByte= CellUtil.cloneValue (result.getColumnLatestCell ("cf1".getBytes (),"info".getBytes ()));
        Person.User user=  Person.User.parseFrom (personByte);
        System.out.println (user.getAddress () );
      //  System.out.println (user.toString () );
     }

}



