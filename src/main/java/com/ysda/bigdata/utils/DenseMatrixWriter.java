package com.ysda.bigdata.utils;

import com.ysda.bigdata.als.local.IDenseMatrix;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by xakl on 13.04.2016.
 */
public class DenseMatrixWriter {
    public void writeMatrix(String filePath, IDenseMatrix matrix) throws IOException {
        double[][] data = matrix.getData();
        int rows = data.length;
        int cols = data[0].length;
        try(FileWriter fileWriter = new FileWriter(filePath)) {
            fileWriter.write(String.format("%s %s\n", rows, cols));
            for (double[] row : data) {
                StringBuilder rowLine = new StringBuilder();
                rowLine.append(row[0]);
                for (int i = 1; i < cols; i++) {
                    rowLine.append('\t');
                    rowLine.append(row[i]);
                }
                rowLine.append('\n');
                fileWriter.write(rowLine.toString());
            }
        }
    }
}
