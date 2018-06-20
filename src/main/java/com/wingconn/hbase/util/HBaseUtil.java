package com.wingconn.hbase.util;


import com.wingconn.hbase.entity.CellEntity;
import com.wingconn.hbase.entity.ColumnFamilyEntity;
import com.wingconn.hbase.entity.HBasePageModel;
import com.wingconn.hbase.entity.QualiferEntity;
import com.wingconn.hbase.probuf.Person;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
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
            closeTable (table);
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
           closeTable (table);
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
            closeTable (table);
            closeConnect (conn);
        }
        return results;
    }

    /**
     * 根据rowKey查询数据
     * @param tableName
     * @param rowKey
     * @return
     */
    public static Result findByRowKey(String tableName,String rowKey){
        Connection conn = getConnection ( );
        Table table=null;
        Result result = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Get get=new Get (Bytes.toBytes(String.valueOf(rowKey)));
            result= table.get (get);
        } catch (IOException e) {
            e.printStackTrace ( );
        }finally {
            closeTable (table);
            closeConnect (conn);
        }
        return result;
    }


    /**
     * 分页检索表数据。<br>
     * （如果在创建表时为此表指定了非默认的命名空间，则需拼写上命名空间名称，格式为【namespace:tablename】）。
     * @param tableName 表名称(*)。
     * @param startRowKey 起始行键(可以为空，如果为空，则从表中第一行开始检索)。
     * @param endRowKey 结束行键(可以为空)。
     * @param filterList 检索条件过滤器集合(不包含分页过滤器；可以为空)。
     * @param maxVersions 指定最大版本数【如果为最大整数值，则检索所有版本；如果为最小整数值，则检索最新版本；否则只检索指定的版本数】。
     * @param pageModel 分页模型(*)。
     * @return 返回HBasePageModel分页对象。
     */
    public static HBasePageModel scanResultByPageFilter(String tableName, byte[] startRowKey, byte[] endRowKey, FilterList filterList, int maxVersions, HBasePageModel pageModel) {
        if(pageModel == null) {
            pageModel = new HBasePageModel(10);
        }
        if(maxVersions <= 0 ) {
            //默认只检索数据的最新版本
            maxVersions = Integer.MIN_VALUE;
        }
        pageModel.initStartTime();
        pageModel.initEndTime();
        if(StringUtils.isBlank(tableName)) {
            return pageModel;
        }
        Connection conn = getConnection ( );
        Table table=null;
        try {
            //根据HBase表名称，得到HTable表对象，这里用到了笔者本人自己构建的一个表信息管理类。
            table = conn.getTable(TableName.valueOf(tableName));
            int tempPageSize = pageModel.getPageSize();
            boolean isEmptyStartRowKey = false;
            if(startRowKey == null) {
                //地区第一行数据
                Result firstResult = selectFirstResultRow(tableName, filterList);
                if(firstResult.isEmpty()) {
                    return pageModel;
                }
                startRowKey = firstResult.getRow();
            }
            if(pageModel.getPageStartRowKey() == null) {
                isEmptyStartRowKey = true;
                pageModel.setPageStartRowKey(startRowKey);
            } else {
                if(pageModel.getPageEndRowKey() != null) {
                    pageModel.setPageStartRowKey(pageModel.getPageEndRowKey());
                }
                //从第二页开始，每次都多取一条记录，因为第一条记录是要删除的。
                tempPageSize += 1;
            }

            Scan scan = new Scan();
            scan.setStartRow(pageModel.getPageStartRowKey());
            if(endRowKey != null) {
                scan.setStopRow(endRowKey);
            }
            PageFilter pageFilter = new PageFilter(pageModel.getPageSize() + 1);
            if(filterList != null) {
                filterList.addFilter(pageFilter);
                scan.setFilter(filterList);
            } else {
                scan.setFilter(pageFilter);
            }
            if(maxVersions == Integer.MAX_VALUE) {
                scan.setMaxVersions();
            } else if(maxVersions == Integer.MIN_VALUE) {

            } else {
                scan.setMaxVersions(maxVersions);
            }
            ResultScanner scanner = table.getScanner(scan);
            List<Result> resultList = new ArrayList<Result>();
            int index = 0;
            for(Result rs : scanner.next(tempPageSize)) {
                if(isEmptyStartRowKey == false && index == 0) {
                    index += 1;
                    continue;
                }
                if(!rs.isEmpty()) {
                    resultList.add(rs);
                }
                index += 1;
            }
            scanner.close();
            pageModel.setResultList(resultList);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int pageIndex = pageModel.getPageIndex() + 1;
        pageModel.setPageIndex(pageIndex);
        if(pageModel.getResultList().size() > 0) {
            //获取本次分页数据首行和末行的行键信息
            byte[] pageStartRowKey = pageModel.getResultList().get(0).getRow();
            byte[] pageEndRowKey = pageModel.getResultList().get(pageModel.getResultList().size() - 1).getRow();
            pageModel.setPageStartRowKey(pageStartRowKey);
            pageModel.setPageEndRowKey(pageEndRowKey);
        }
        int queryTotalCount = pageModel.getQueryTotalCount() + pageModel.getResultList().size();
        pageModel.setQueryTotalCount(queryTotalCount);
        pageModel.initEndTime();
        return pageModel;
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

    public static void closeTable(Table table){
        if(null!=table){
            try {
                table.close ();
            } catch (IOException e) {
                e.printStackTrace ( );
            }
        }
    }


    /**
     * 检索指定表的第一行记录。<br>
     * （如果在创建表时为此表指定了非默认的命名空间，则需拼写上命名空间名称，格式为【namespace:tablename】）。
     * @param tableName 表名称(*)。
     * @param filterList 过滤器集合，可以为null。
     * @return
     */
    private static Result selectFirstResultRow(String tableName,FilterList filterList) {
        if(StringUtils.isBlank(tableName)) return null;
        Table table = null;
        Connection conn = getConnection ( );
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            if(filterList != null) {
                scan.setFilter(filterList);
            }
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            int index = 0;
            while(iterator.hasNext()) {
                Result rs = iterator.next();
                if(index == 0) {
                    scanner.close();
                    return rs;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

}