package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.BaseAlsInitConfig;
import com.ysda.bigdata.als.ISparseMatrix;

/**
 * Created by kwetril on 5/14/16.
 */
public class LocalAslInitConfig extends BaseAlsInitConfig {
    public ISparseMatrix ratingMatrix;
    public ISparseMatrix transposedRatingMatrix;
}
