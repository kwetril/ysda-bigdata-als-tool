package com.ysda.bigdata.als.local;

import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Created by kwetril on 5/15/16.
 */
public class FactorMatrix implements Serializable {
    private HashMap<String, double[]> data;
    private Random generator = new Random();
    private int numFactors;

    public FactorMatrix(int numFactors) {
        data = new HashMap<>();
        this.numFactors = numFactors;
    }

    public FactorMatrix(HashMap<String, double[]> data, int numFactors) {
        this.data = data;
        this.numFactors = numFactors;
    }

    public FactorMatrix(List<Tuple2<String, double[]>> userFactorsList) {
        data = new HashMap<>();
        for (Tuple2<String, double[]> factors : userFactorsList) {
            data.put(factors._1, factors._2);
            numFactors = factors._2.length;
        }
    }

    public DenseMatrix getSubmatrix(String[] rows) {
        double[][] submatrixData = new double[rows.length][];
        for (int i = 0; i < rows.length; i++) {
            submatrixData[i] = getRow(rows[i]);
        }
        return new DenseMatrix(submatrixData);
    }

    public double[] getRow(String row) {
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

    public void setRow(String row, double[] values) {
        synchronized (data) {
            data.put(row, values);
        }
    }

    public HashMap<String, double[]> getData() {
        return data;
    }

    public int getNumFactors() {
        return numFactors;
    }
}
