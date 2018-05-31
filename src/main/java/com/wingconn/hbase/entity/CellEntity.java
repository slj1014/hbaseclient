package com.wingconn.hbase.entity;

/**
 * Created by albert.shen on 2018/5/31.
 */
public class CellEntity {
    private String cellName;
    private Object cellValue;

    public String getCellName() {
        return cellName;
    }

    public void setCellName(String cellName) {
        this.cellName = cellName;
    }

    public Object getCellValue() {
        return cellValue;
    }

    public void setCellValue(Object cellValue) {
        this.cellValue = cellValue;
    }
}
