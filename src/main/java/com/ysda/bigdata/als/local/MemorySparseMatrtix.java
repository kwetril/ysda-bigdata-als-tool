package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.ISparseMatrix;
import com.ysda.bigdata.als.SparseRow;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by xakl on 13.04.2016.
 */
public class MemorySparseMatrtix implements ISparseMatrix {
    ArrayList<SparseRow> rows;
    int numRows;
    int numCols;

    public MemorySparseMatrtix(int numRows, int numCols, ArrayList<SparseRow> rows) {
        this.numRows = numRows;
        this.numCols = numCols;
        this.rows = rows;
    }

    @Override
    public Iterator<SparseRow> iterator() {
        return rows.iterator();
    }

    @Override
    public int getNumRows() {
        return numRows;
    }

    @Override
    public int getNumCols() {
        return numCols;
    }
}
