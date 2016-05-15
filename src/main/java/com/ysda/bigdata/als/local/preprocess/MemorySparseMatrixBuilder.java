package com.ysda.bigdata.als.local.preprocess;

import com.ysda.bigdata.als.local.ISparseMatrix;
import com.ysda.bigdata.als.local.SparseRow;
import com.ysda.bigdata.als.local.SparseRowElement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xakl on 13.04.2016.
 */
public class MemorySparseMatrixBuilder {
    HashMap<String, ArrayList<SparseRowElement>> data;

    public MemorySparseMatrixBuilder() {
        data = new HashMap<>();
    }

    public MemorySparseMatrixBuilder addElement(String row, String col, double value) {
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
        for (Map.Entry<String, ArrayList<SparseRowElement>> entry : data.entrySet()) {
            String[] indices = new String[entry.getValue().size()];
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
        return new MemorySparseMatrtix(rows);
    }
}
