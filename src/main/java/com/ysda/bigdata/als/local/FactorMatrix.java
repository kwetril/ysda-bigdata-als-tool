package com.ysda.bigdata.als.local;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Created by kwetril on 5/15/16.
 */
public class FactorMatrix {
    private HashMap<String, double[]> data;
    private Random generator;
    private int numFactors;

    public FactorMatrix(int numFactors) {
        this.numFactors = numFactors;
        this.data = new HashMap<>();
        this.generator = new Random();
    }

    public FactorMatrix(HashMap<String, double[]> data, int numFactors) {
        this.data = data;
        this.numFactors = numFactors;
    }

    DenseMatrix getSubmatrix(String[] rows) {
        double[][] submatrixData = new double[rows.length][];
        for (int i = 0; i < rows.length; i++) {
            submatrixData[i] = getRow(rows[i]);
        }
        return new DenseMatrix(submatrixData);
    }

    double[] getRow(String row) {
        if (data.containsKey(row)) {
            return data.get(row);
        } else {
            double[] result = new double[numFactors];
            for (int i = 0; i < numFactors; i++) {
                result[i] = generator.nextDouble();
            }
            setRow(row, result);
            return getRow(row);
        }
    }

    void setRow(String row, double[] values) {
        synchronized (data) {
            data.put(row, values);
        }
    }

    HashMap<String, double[]> getData() {
        return data;
    }

    int getNumFactors() {
        return numFactors;
    }
}
