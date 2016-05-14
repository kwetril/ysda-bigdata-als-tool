package com.ysda.bigdata.als.local;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by xakl on 13.04.2016.
 */
public class SparseRow {
    private String rowIndex;
    private String[] colIndices;
    private  double[] values;

    public SparseRow(String rowIndex, String[] colIndices, double[] values) {
        this.rowIndex = rowIndex;
        this.colIndices = colIndices;
        this.values = values;
    }

    public int getNumElements() {
        return colIndices.length;
    }

    public String getRowIndex() {
        return rowIndex;
    }

    public String[] getColIndices() {
        return colIndices;
    }

    public double[] getValues() {
        return values;
    }
}
