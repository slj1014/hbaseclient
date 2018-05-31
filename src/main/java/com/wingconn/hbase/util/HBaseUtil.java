package com.wingconn.hbase.util;


import com.wingconn.hbase.entity.CellEntity;
import com.wingconn.hbase.entity.ColumnFamilyEntity;
import com.wingconn.hbase.entity.QualiferEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by albert.shen on 2018/5/31.
 */
public class HBaseUtil {

    private static final Logger logger = LoggerFactory.getLogger (HBaseUtil.class);
    private static Configuration conf;
    private static Connection conn;

    /**
     * 初始化对象
     */
    public static void init(String zkHosts) {
        try {
            if (conf == null) {
                conf = HBaseConfiguration.create ( );
                conf.set ("hbase.zookeeper.quorum", zkHosts);
            }
        } catch (Exception e) {
            logger.error ("HBase Configuration Initialization failure !");
            throw new RuntimeException (e);
        }
    }


    /**
     * 获得链接
     *
     * @return
     */
    public static synchronized Connection getConnection() {
        try {
            if (conn == null || conn.isClosed ( )) {
                conn = ConnectionFactory.createConnection (conf);
            }
        } catch (IOException e) {
            logger.error ("HBase 建立链接失败 ", e);
        }
        return conn;
    }

    /**
     * 创建表
     */
    public static void createTable(String tableName, String[] columnFamilies) {
        Connection conn = getConnection ( );
        HBaseAdmin admin = null;
        try {
            admin = (HBaseAdmin) conn.getAdmin ( );
            if (admin.tableExists (tableName)) {
                logger.warn ("Table: {} is exists!", tableName);
                return;
            }
            HTableDescriptor tableDesc = new HTableDescriptor (TableName.valueOf (tableName));
            for (int i = 0; i < columnFamilies.length; i++) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor (columnFamilies[i]);
                hColumnDescriptor.setMaxVersions (1);
                tableDesc.addFamily (hColumnDescriptor);
            }
            admin.createTable (tableDesc);
        } catch (IOException e) {
            e.printStackTrace ( );
        } finally {
            closeAdmin (admin);
            closeConnect (conn);
        }
    }


    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(String tableName) {
        Connection conn = getConnection ( );
        HBaseAdmin admin =null;
        try {
             admin = (HBaseAdmin) conn.getAdmin ( );
            if (!admin.tableExists (tableName)) {
                logger.warn ("Table: {} is not exists!", tableName);
                return;
            }
            admin.disableTable (tableName);
            admin.deleteTable (tableName);
            logger.info ("Table: {} delete success!", tableName);
        }catch (Exception e){
            e.printStackTrace ();
        }
        finally {
            closeAdmin (admin);
            closeConnect (conn);
        }
    }

    /**
     * 添加数据
     * {
     * //一条数据
     * [
     * {
     * "cf1":{
     * "name","xiaoming",
     * "age","12"
     * },
     * "cf2":{
     * "address":"suzhou",
     * "salary":"10000"
     * }
     * ]
     * }
     * }
     */
    public static void puData(String tableName, String rowKey, List<ColumnFamilyEntity> columnFamilies) {
        Connection conn = getConnection ( );
        List<Put> puts = new ArrayList<Put> ( );
        for (ColumnFamilyEntity columnFamily : columnFamilies) {
            Put put = new Put (rowKey.getBytes ( ));
            List<QualiferEntity> qualifeies = columnFamily.getQualifers ( );
            for (QualiferEntity qualifer : qualifeies) {
                String qualiferName = qualifer.getQualiferName ( );
                List<CellEntity> cells = qualifer.getQualiferValue ( );
                for (CellEntity cell : cells) {
                    put.addColumn (qualiferName.getBytes ( ), cell.getCellName ( ).getBytes ( ), cell.getCellValue ( ).toString ( ).getBytes ( ));
                }
            }
            puts.add (put);
        }
        Table table=null;
        try {
             table = conn.getTable(TableName.valueOf(tableName));
            table.put (puts);
        } catch (IOException e) {
            e.printStackTrace ( );
        }finally {
            colseTable(table);
            closeConnect (conn);
        }
    }

    /**
     * 查询表中所有记录
     */
    public static List<Cell[]>  findAll(String tableName){
        Connection conn = getConnection ( );
        Table table=null;
        ResultScanner rs=null;
        List<Cell[]> cellList=new ArrayList<Cell[]> ();
        try {
              table = conn.getTable(TableName.valueOf(tableName));
              rs = table.getScanner(new Scan());
             for (Result r : rs) {
                Cell[] cell = r.rawCells();
                cellList.add (cell);
            }
        } catch (IOException e) {
            e.printStackTrace ( );
        }finally {
            rs.close ();
           colseTable (table);
           closeConnect (conn);
        }
        return cellList;
    }


    /**
     * 获取多行数据
     * @param tableName
     * @param rowKeys
     * @return
     * @throws Exception
     */
    public static <T> Result[] findByRowKeys(String tableName, List<T> rowKeys)  {
        Connection conn = getConnection ( );
        Table table=null;
        List<Get> gets = new ArrayList<Get> ();
        Result[] results = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            for (T rowKey : rowKeys) {
                if(rowKey!=null){
                    gets.add(new Get(Bytes.toBytes(String.valueOf(rowKey))));
                }else{
                    throw new RuntimeException("hbase have no data");
                }
            }
            if (gets.size() > 0) {
                results = table.get(gets);
            }

        } catch (IOException e) {
            e.printStackTrace ( );
        }finally {
            colseTable (table);
            closeConnect (conn);
        }
        return results;
    }


    /**
     * 关闭连接
     *
     * @throws IOException
     */
    public static void closeConnect(Connection conn) {
        if (null != conn) {
            try {
                conn.close ( );
            } catch (Exception e) {
                logger.error ("closeConnect failure !", e);
            }
        }
    }

    /**
     * 关闭admin
     *
     * @throws IOException
     */
    public static void closeAdmin(HBaseAdmin admin) {
        if (null != admin) {
            try {
                admin.close ( );
            } catch (Exception e) {
                logger.error ("HBaseAdmin failure !", e);
            }
        }
    }

    public static void colseTable(Table table){
        if(null!=table){
            try {
                table.close ();
            } catch (IOException e) {
                e.printStackTrace ( );
            }
        }
    }
}