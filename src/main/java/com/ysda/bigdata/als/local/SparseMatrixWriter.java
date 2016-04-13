package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.ISparseMatrix;
import com.ysda.bigdata.als.SparseRow;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by xakl on 13.04.2016.
 */
public class SparseMatrixWriter {
    public void writeMatrix(String path, ISparseMatrix matrix) throws IOException {
        try (FileWriter fileWriter = new FileWriter(path)) {
            fileWriter.write(String.format("%s %s\n", matrix.getNumRows(), matrix.getNumCols()));
            for (SparseRow row : matrix) {
                int[] indices = row.getColIndices();
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
