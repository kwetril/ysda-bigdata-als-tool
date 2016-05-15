package com.ysda.bigdata.als.spark;

import com.ysda.bigdata.als.BaseAlsInitConfig;
import com.ysda.bigdata.als.BaseAlsModel;
import com.ysda.bigdata.als.local.DenseMatrix;
import com.ysda.bigdata.als.local.FactorMatrix;
import com.ysda.bigdata.utils.RatingDataRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kwetril on 5/14/16.
 */
public class SparkAlsModel extends BaseAlsModel implements Serializable {
    private JavaPairRDD<String, Tuple2<String[], double[]>> userRatings;
    private JavaPairRDD<String, Tuple2<String[], double[]>> itemRatings;
    private JavaSparkContext context;

    private static final PairFunction<Tuple2<String, Iterable<RatingDataRecord>>, String, Tuple2<String[], double[]>> iterableItemsToArrayMap
            = new PairFunction<Tuple2<String, Iterable<RatingDataRecord>>, String, Tuple2<String[], double[]>>() {
        private ArrayList<String> items = new ArrayList<>();

        @Override
        public Tuple2<String, Tuple2<String[], double[]>> call(Tuple2<String, Iterable<RatingDataRecord>> value) throws Exception {
            for (RatingDataRecord record : value._2) {
                items.add(record.item);
            }
            String[] itemsArray = new String[items.size()];
            itemsArray = items.toArray(itemsArray);

            double[] ratingsArray = new double[items.size()];
            int i = 0;
            for (RatingDataRecord record : value._2) {
                ratingsArray[i] = record.rating;
                i++;
            }
            items.clear();
            return new Tuple2<>(value._1,
                    new Tuple2<>(itemsArray, ratingsArray));
        }
    };

    private static final PairFunction<Tuple2<String, Iterable<RatingDataRecord>>, String, Tuple2<String[], double[]>> iterableUsersToArrayMap
            = new PairFunction<Tuple2<String, Iterable<RatingDataRecord>>, String, Tuple2<String[], double[]>>() {
        private ArrayList<String> users = new ArrayList<>();

        @Override
        public Tuple2<String, Tuple2<String[], double[]>> call(Tuple2<String, Iterable<RatingDataRecord>> value) throws Exception {
            for (RatingDataRecord record : value._2) {
                users.add(record.user);
            }
            String[] itemsArray = new String[users.size()];
            itemsArray = users.toArray(itemsArray);

            double[] ratingsArray = new double[users.size()];
            int i = 0;
            for (RatingDataRecord record : value._2) {
                ratingsArray[i] = record.rating;
                i++;
            }
            users.clear();
            return new Tuple2<>(value._1,
                    new Tuple2<>(itemsArray, ratingsArray));
        }
    };
    private static final Function<RatingDataRecord, String> takeUser = new Function<RatingDataRecord, String>() {
        @Override
        public String call(RatingDataRecord record) throws Exception {
            return record.user;
        }
    };

    private static final Function<RatingDataRecord, String> takeItem = new Function<RatingDataRecord, String>() {
        @Override
        public String call(RatingDataRecord record) throws Exception {
            return record.item;
        }
    };

    @Override
    public void init(BaseAlsInitConfig config) {
        SparkAlsInitConfig sparkConfig = (SparkAlsInitConfig) config;
        numFactors = sparkConfig.numFactors;
        regCoefficient = sparkConfig.regCoefficient;
        rowFactorsMatrix = new FactorMatrix(numFactors);
        colFactorsMatrix = new FactorMatrix(numFactors);
        context = sparkConfig.sparkContext;

        sparkConfig.records.persist(StorageLevel.MEMORY_AND_DISK());

        userRatings = sparkConfig.records.groupBy(takeUser)
                .mapToPair(iterableItemsToArrayMap).persist(StorageLevel.MEMORY_AND_DISK());

        itemRatings = sparkConfig.records.groupBy(takeItem)
                .mapToPair(iterableUsersToArrayMap).persist(StorageLevel.MEMORY_AND_DISK());

        sparkConfig.records.unpersist();
    }

    @Override
    public void train(int numIterations) {
        for (int i = 0; i < numIterations; i++) {
            doIteration();
        }
    }

    static class MapRatingsFunction implements Function<Tuple2<String, Tuple2<String[], double[]>>, Tuple2<String, double[]>> {
        private Broadcast<FactorMatrix> broadcastedFactors;
        private double regCoefficient;

        public MapRatingsFunction(Broadcast<FactorMatrix> broadcastedFactors, double regCoefficient) {
            this.broadcastedFactors = broadcastedFactors;
            this.regCoefficient = regCoefficient;
        }

        @Override
        public Tuple2<String, double[]> call(Tuple2<String, Tuple2<String[], double[]>> value) throws Exception {
            String[] items = value._2._1;
            double[] ratings = value._2._2;
            DenseMatrix itemFactorsSubmatrix = broadcastedFactors.getValue().getSubmatrix(items);
            DenseMatrix transposedItemFactorsSubmatrix = itemFactorsSubmatrix.transpose();
            double[] result = transposedItemFactorsSubmatrix
                    .multiply(itemFactorsSubmatrix)
                    .addDiag(regCoefficient)
                    .inverse()
                    .multiply(transposedItemFactorsSubmatrix)
                    .multiply(ratings);
            return new Tuple2<>(value._1, result);
        }
    }

    private void doIteration() {
        final Broadcast<FactorMatrix> broadcastedItemFactors = context.broadcast(colFactorsMatrix);
        final MapRatingsFunction mapRatingsFunction = new MapRatingsFunction(broadcastedItemFactors, regCoefficient);
        List<Tuple2<String, double[]>> userFactorsList = userRatings.map(mapRatingsFunction).collect();
        rowFactorsMatrix = new FactorMatrix(userFactorsList);

        final Broadcast<FactorMatrix> broadcastedUserFactors = context.broadcast(rowFactorsMatrix);
        List<Tuple2<String, double[]>> itemFactorsList = itemRatings.map(
                new MapRatingsFunction(broadcastedUserFactors, regCoefficient)).collect();
        colFactorsMatrix = new FactorMatrix(itemFactorsList);
    }

    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException{
        out.writeInt(super.numFactors);
        out.writeObject(rowFactorsMatrix);
        out.writeObject(colFactorsMatrix);
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException {
        numFactors = in.readInt();
        try {
            rowFactorsMatrix = (FactorMatrix) in.readObject();
            colFactorsMatrix = (FactorMatrix) in.readObject();
        } catch (ClassNotFoundException e) {
            rowFactorsMatrix = null;
            colFactorsMatrix = null;
        }
    }
}
