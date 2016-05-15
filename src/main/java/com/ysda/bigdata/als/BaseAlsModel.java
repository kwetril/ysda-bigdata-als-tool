package com.ysda.bigdata.als;

import com.ysda.bigdata.als.BaseAlsInitConfig;
import com.ysda.bigdata.als.IAlsModel;
import com.ysda.bigdata.als.local.FactorMatrix;
import com.ysda.bigdata.utils.FastScanner;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kwetril on 5/15/16.
 */
public abstract class BaseAlsModel implements IAlsModel {
    protected FactorMatrix rowFactorsMatrix;
    protected FactorMatrix colFactorsMatrix;
    protected int numFactors;
    protected double regCoefficient;

    @Override
    public void init(BaseAlsInitConfig config) {
        numFactors = config.numFactors;
        regCoefficient = config.regCoefficient;
        rowFactorsMatrix = new FactorMatrix(numFactors);
        colFactorsMatrix = new FactorMatrix(numFactors);
    }


    @Override
    public void save(String path) throws IOException {
        try(FileWriter fileWriter = new FileWriter(path)) {
            writeMatrix(fileWriter, rowFactorsMatrix);
            writeMatrix(fileWriter, colFactorsMatrix);
        }
    }

    private void writeMatrix(FileWriter fileWriter, FactorMatrix matrix) throws IOException {
        HashMap<String, double[]> data = matrix.getData();
        int rows = data.size();
        int cols = matrix.getNumFactors();
        fileWriter.write(String.format("%s %s\n", rows, cols));
        for (Map.Entry<String, double[]> row : data.entrySet()) {
            StringBuilder rowLine = new StringBuilder();
            rowLine.append(row.getKey());
            for (double value : row.getValue()) {
                rowLine.append('\t');
                rowLine.append(value);
            }
            rowLine.append('\n');
            fileWriter.write(rowLine.toString());
        }
    }

    private FactorMatrix readMatrix(FastScanner scanner) throws IOException {
        int rows = scanner.nextInt();
        int cols = scanner.nextInt();
        HashMap<String, double[]> data = new HashMap<>();
        int numFactors = cols;
        for (int i = 0; i < rows; i++) {
            String key = scanner.next();
            double[] values = new double[cols];
            for (int j = 0; j < cols; j++) {
                values[j] = scanner.nextDouble();
            }
            data.put(key, values);
        }
        return new FactorMatrix(data, numFactors);
    }

    @Override
    public void load(String path) throws IOException {
        FastScanner scanner = new FastScanner(path);
        rowFactorsMatrix = readMatrix(scanner);
        colFactorsMatrix = readMatrix(scanner);
        numFactors = rowFactorsMatrix.getNumFactors();
    }

    @Override
    public double predict(String user, String item) {
        double[] userValues = rowFactorsMatrix.getRow(user);
        double[] itemValues = colFactorsMatrix.getRow(item);
        double result = 0;
        for (int i = 0; i < userValues.length; i++) {
            result += userValues[i] * itemValues[i];
        }
        return result;
    }
}
