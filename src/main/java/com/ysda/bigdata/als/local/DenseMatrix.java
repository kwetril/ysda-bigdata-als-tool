package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.IDenseMatrix;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

import java.util.ArrayList;

/**
 * Created by xakl on 13.04.2016.
 */
public class DenseMatrix implements IDenseMatrix {
    private RealMatrix matrix;

    public DenseMatrix(int rows, int cols) {
        matrix = new Array2DRowRealMatrix(rows, cols);
    }

    private DenseMatrix(RealMatrix matrix) {
        this.matrix = matrix;
    }

    @Override
    public IDenseMatrix getSubmatrix(int[] rowIndices) {
        double[][] result = new double[rowIndices.length][];
        for (int i = 0; i < rowIndices.length; i++) {
            result[i] = matrix.getRow(rowIndices[i]);
        }
        RealMatrix resultMatrix = new Array2DRowRealMatrix(result);
        return new DenseMatrix(resultMatrix);
    }

    @Override
    public IDenseMatrix multiply(IDenseMatrix anotherMatrix) {
        return new DenseMatrix(matrix.multiply(((DenseMatrix)anotherMatrix).matrix));
    }

    @Override
    public IDenseMatrix addDiag(double value) {
        for (int i = 0; i < matrix.getRowDimension(); i++) {
            matrix.addToEntry(i, i, value);
        }
        return this;
    }

    @Override
    public double[] multiply(double[] vector) {
        return this.matrix.operate(vector);
    }

    @Override
    public IDenseMatrix setRow(int rowIndex, double[] rowData) {
        this.matrix.setRow(rowIndex, rowData);
        return this;
    }

    @Override
    public IDenseMatrix inverse() {
        return new DenseMatrix(MatrixUtils.inverse(this.matrix));
    }

    @Override
    public IDenseMatrix transpose() {
        return new DenseMatrix(this.matrix.transpose());
    }
}
