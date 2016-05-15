package com.ysda.bigdata.als.local.preprocess;

import com.ysda.bigdata.als.local.ISparseMatrix;
import com.ysda.bigdata.als.local.SparseRow;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by xakl on 13.04.2016.
 */
public class SparseMatrixWriter {
    public void writeMatrix(String path, ISparseMatrix matrix) throws IOException {
        try (FileWriter fileWriter = new FileWriter(path)) {
            for (SparseRow row : matrix) {
                String[] indices = row.getColIndices();
                double[] values = row.getValues();
                StringBuilder rowLine = new StringBuilder();
                rowLine.append(String.format("%s", row.getRowIndex()));
                for (int i = 0; i < row.getNumElements(); i++) {
                    rowLine.append(String.format(" %s:%s", indices[i], values[i]));
                }
                rowLine.append("\n");
                fileWriter.write(rowLine.toString());
            }
        }
    }
}
