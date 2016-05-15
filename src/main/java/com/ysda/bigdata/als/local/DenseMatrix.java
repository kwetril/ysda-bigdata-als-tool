package com.ysda.bigdata.als.local;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

/**
 * Created by xakl on 13.04.2016.
 */
public class DenseMatrix {
    private RealMatrix matrix;

    public DenseMatrix(double[][] data) {
        matrix = new Array2DRowRealMatrix(data);
    }

    private DenseMatrix(RealMatrix matrix) {
        this.matrix = matrix;
    }

    public DenseMatrix multiply(DenseMatrix anotherMatrix) {
        return new DenseMatrix(matrix.multiply(((DenseMatrix)anotherMatrix).matrix));
    }

    public DenseMatrix addDiag(double value) {
        for (int i = 0; i < matrix.getRowDimension(); i++) {
            matrix.addToEntry(i, i, value);
        }
        return this;
    }

    public double[] multiply(double[] vector) {
        return this.matrix.operate(vector);
    }

    public DenseMatrix inverse() {
        return new DenseMatrix(MatrixUtils.inverse(this.matrix));
    }

    public DenseMatrix transpose() {
        return new DenseMatrix(this.matrix.transpose());
    }

    public double[][] getData() {
        return matrix.getData();
    }
}
