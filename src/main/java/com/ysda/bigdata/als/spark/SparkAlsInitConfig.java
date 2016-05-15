package com.ysda.bigdata.als.spark;

import com.ysda.bigdata.als.BaseAlsInitConfig;
import com.ysda.bigdata.utils.RatingDataRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by kwetril on 5/14/16.
 */
public class SparkAlsInitConfig extends BaseAlsInitConfig {
    public JavaRDD<RatingDataRecord> records;
    public JavaSparkContext sparkContext;
}
