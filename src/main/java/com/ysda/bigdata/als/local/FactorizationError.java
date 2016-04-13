package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.IDenseMatrix;
import com.ysda.bigdata.als.ISparseMatrix;
import com.ysda.bigdata.als.SparseRow;

/**
 * Created by kwetril on 4/13/16.
 */
public class FactorizationError {
    public double computeMSE(ISparseMatrix ratingMatrix, IDenseMatrix rowFactors, IDenseMatrix colFactors) {
        double result = 0;
        int numberOfElements = 0;
        for (SparseRow row : ratingMatrix) {
            int[] indices = row.getColIndices();
            double[] values = row.getValues();
            for (int i = 0; i < row.getNumElements(); i++) {
                double reconstructedVal = rowFactors.innerRowsProduct(row.getRowIndex(), colFactors, indices[i]);
                double diff = reconstructedVal - values[i];
                result += diff * diff;
            }
            numberOfElements += row.getNumElements();
        }
        return result / numberOfElements;
    }
}
