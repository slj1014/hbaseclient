package com.wingconn.hbase.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by albert.shen on 2018/5/31.
 */
public class ColumnFamilyEntity {
    private List<QualiferEntity> qualifers=new ArrayList ();

    public List<QualiferEntity> getQualifers() {
        return qualifers;
    }

    public void setQualifers(List<QualiferEntity> qualifers) {
        this.qualifers = qualifers;
    }
}
