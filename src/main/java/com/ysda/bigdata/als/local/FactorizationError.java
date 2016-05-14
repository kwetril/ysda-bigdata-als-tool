package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.IAlsModel;

/**
 * Created by kwetril on 4/13/16.
 */
public class FactorizationError {
    public double computeMSE(ISparseMatrix ratings, IAlsModel alsModel) {
        double result = 0;
        int numberOfElements = 0;
        for (SparseRow row : ratings) {
            String user = row.getRowIndex();
            String[] items = row.getColIndices();
            double[] ratingValues = row.getValues();
            for (int i = 0; i < items.length; i++) {
                double reconstructedVal = alsModel.predict(user, items[i]);
                double diff = reconstructedVal - ratingValues[i];
                result += diff * diff;
            }
            numberOfElements += items.length;
        }
        return result / numberOfElements;
    }
}
