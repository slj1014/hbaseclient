package com.wingconn.hbase.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by albert.shen on 2018/5/31.
 */
public class QualiferEntity {
    private String qualiferName;
    private List<CellEntity> qualiferValue=new ArrayList<CellEntity> ();

    public String getQualiferName() {
        return qualiferName;
    }

    public void setQualiferName(String qualiferName) {
        this.qualiferName = qualiferName;
    }

    public List<CellEntity> getQualiferValue() {
        return qualiferValue;
    }

    public void setQualiferValue(List<CellEntity> qualiferValue) {
        this.qualiferValue = qualiferValue;
    }
}
