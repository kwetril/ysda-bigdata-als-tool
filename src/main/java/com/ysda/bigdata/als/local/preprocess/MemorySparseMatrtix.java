package com.ysda.bigdata.als.local.preprocess;

import com.ysda.bigdata.als.local.ISparseMatrix;
import com.ysda.bigdata.als.local.SparseRow;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by xakl on 13.04.2016.
 */
public class MemorySparseMatrtix implements ISparseMatrix {
    ArrayList<SparseRow> rows;

    public MemorySparseMatrtix(ArrayList<SparseRow> rows) {
        this.rows = rows;
    }

    @Override
    public Iterator<SparseRow> iterator() {
        return rows.iterator();
    }
}
