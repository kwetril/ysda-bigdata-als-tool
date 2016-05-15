package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.BaseAlsModel;
import com.ysda.bigdata.als.IAlsModel;
import com.ysda.bigdata.utils.RatingDataRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Int;
import scala.Tuple2;

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

    private static class mapMSE implements Function<RatingDataRecord, Tuple2<Integer, Double>> {
        private BaseAlsModel alsModel;

        public mapMSE(BaseAlsModel alsModel) {
            this.alsModel = alsModel;
        }

        @Override
        public Tuple2<Integer, Double> call(RatingDataRecord ratingDataRecord) throws Exception {
            double reconstructedVal = alsModel.predict(ratingDataRecord.user, ratingDataRecord.item);
            double diff = reconstructedVal - ratingDataRecord.rating;
            return new Tuple2<>(1, diff * diff);
        }
    }

    private static Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>> avgReduce =
            new Function2<Tuple2<Integer, Double>, Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
        @Override
        public Tuple2<Integer, Double> call(Tuple2<Integer, Double> acc, Tuple2<Integer, Double> val) {
            return new Tuple2<>(acc._1 + val._1, acc._2 + val._2);
        }
    };

    public double computeMSE(JavaRDD<RatingDataRecord> ratings, final BaseAlsModel alsModel) {
        Tuple2<Integer, Double> mse = ratings.map(new mapMSE(alsModel)).reduce(avgReduce);
        return mse._2 / mse._1;
    }
}
