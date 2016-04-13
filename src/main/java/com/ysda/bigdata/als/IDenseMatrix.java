package com.ysda.bigdata.als;

import java.util.ArrayList;

/**
 * Created by xakl on 13.04.2016.
 */
public interface IDenseMatrix {
    IDenseMatrix getSubmatrix(int[] rowIndices);
    IDenseMatrix multiply(IDenseMatrix anotherMatrix);
    IDenseMatrix addDiag(double value);
    double[] multiply(double[] vector);
    IDenseMatrix setRow(int rowIndex, double[] rowData);
    double innerRowsProduct(int rowIndex, IDenseMatrix anotherMatrix, int anotherRowIndex);
    IDenseMatrix inverse();
    IDenseMatrix transpose();
    double[][] getData();
}
