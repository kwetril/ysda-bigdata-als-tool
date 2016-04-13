package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.ISparseMatrix;
import com.ysda.bigdata.als.SparseRow;
import com.ysda.bigdata.als.SparseRowElement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xakl on 13.04.2016.
 */
public class MemorySparseMatrixBuilder {
    HashMap<Integer, ArrayList<SparseRowElement>> data;
    int numRows;
    int numCols;

    public MemorySparseMatrixBuilder() {
        data = new HashMap<>();
    }

    public MemorySparseMatrixBuilder addElement(int row, int col, double value) {
        if (row + 1 > numRows) {
            numRows = row + 1;
        }
        if (col + 1 > numCols) {
            numCols = col + 1;
        }
        SparseRowElement element = new SparseRowElement();
        element.columnIndex = col;
        element.value = value;
        if (data.containsKey(row)) {
            data.get(row).add(element);
        } else {
            ArrayList<SparseRowElement> elements = new ArrayList<>();
            elements.add(element);
            data.put(row, elements);
        }
        return this;
    }

    public ISparseMatrix build() {
        ArrayList<SparseRow> rows = new ArrayList<>();
        for (Map.Entry<Integer, ArrayList<SparseRowElement>> entry : data.entrySet()) {
            int[] indices = new int[entry.getValue().size()];
            double[] values = new double[indices.length];
            int i = 0;
            for (SparseRowElement element : entry.getValue()) {
                indices[i] = element.columnIndex;
                values[i] = element.value;
                i++;
            }
            SparseRow row = new SparseRow(entry.getKey(), indices, values);
            rows.add(row);
        }
        return new MemorySparseMatrtix(numRows, numCols, rows);
    }
}
