import com.wingconn.hbase.entity.CellEntity;
import com.wingconn.hbase.entity.ColumnFamilyEntity;
import com.wingconn.hbase.entity.QualiferEntity;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;
import com.wingconn.hbase.util.HBaseUtil;

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
        HBaseUtil.createTable ("phone",columnFamliy);
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

}
