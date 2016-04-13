package com.ysda.bigdata.als;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by xakl on 13.04.2016.
 */
public class SparseRow {
    private int rowIndex;
    private int[] colIndices;
    private  double[] values;

    public int getNumElements() {
        return colIndices.length;
    }

    public int getRowIndex() {
        return rowIndex;
    }

    public int[] getColIndices() {
        return colIndices;
    }

    public double[] getValues() {
        return values;
    }
}
