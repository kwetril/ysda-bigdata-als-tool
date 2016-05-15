package com.ysda.bigdata;

import com.ysda.bigdata.als.BaseAlsModel;
import com.ysda.bigdata.als.IAlsModel;
import com.ysda.bigdata.als.local.*;
import com.ysda.bigdata.als.local.preprocess.DataToMatrixLocalFileConverter;
import com.ysda.bigdata.als.spark.SparkAlsModel;
import com.ysda.bigdata.als.spark.SparkAlsInitConfig;
import com.ysda.bigdata.utils.AlsToolConfig;
import com.ysda.bigdata.utils.RatingDataRecord;
import com.ysda.bigdata.utils.StopWatch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.File;
import java.io.IOException;

/**
 * Created by xakl on 12.04.2016.
 */
public class AlsTool {
    public static void main(String[] args) {
        AlsToolConfig config = new AlsToolConfig();
        CmdLineParser argsParser = new CmdLineParser(config);
        try {
            argsParser.parseArgument(args);
        } catch (CmdLineException ex) {
            ex.printStackTrace();
            argsParser.printUsage(System.err);
            return;
        }

        if (config.getExecutionMode() == AlsToolConfig.ExecutionMode.LOCAL) {
            RunLocalMode(config);
        } else {
            RunSparkMode(config);
        }
    }

    private static void RunLocalMode(AlsToolConfig config) {
        if (config.getOperation() == AlsToolConfig.OperationType.PREPROCESSING) {
            DataToMatrixLocalFileConverter converter = new DataToMatrixLocalFileConverter();
            converter.doConversion(config);
            return;
        } else {
            try {
                StopWatch timer = new StopWatch();
                timer.start();
                LocalAslInitConfig alsConfig;
                IAlsModel alsModel;
                if (config.getNumWorkers() == 1) {
                    alsConfig = new LocalAslInitConfig();
                    alsModel = new LocalAlsModel();
                } else {
                    LocalMultiThreadAlsInitConfig alsMultiThreadConfig = new LocalMultiThreadAlsInitConfig();
                    alsMultiThreadConfig.numThreads = config.getNumWorkers();
                    alsConfig = alsMultiThreadConfig;
                    alsModel = new LocalMultiThreadAlsModel();
                }
                alsConfig.ratingMatrix = new FileSparseMatrix(config.getInputFilePath());
                alsConfig.transposedRatingMatrix = new FileSparseMatrix(config.getTransposedInputFilePath());
                alsConfig.numFactors = config.getNumFactors();
                alsConfig.regCoefficient = config.getRegCoefficient();
                alsModel.init(alsConfig);
                FactorizationError errorCounter = new FactorizationError();
                StopWatch iterTimer = new StopWatch();
                for (int iter = 1; iter <= 10; iter++) {
                    iterTimer.start();
                    alsModel.train(1);
                    iterTimer.stop();
                    double error = errorCounter.computeMSE(alsConfig.ratingMatrix, alsModel);
                    System.out.printf("Iteration %s\n", iter);
                    System.out.printf("MSE error %s\n", error);
                    System.out.printf("Iteration time %s\n", iterTimer.getElapsedTime());
                }

                File outputFile = new File(config.getOutputDirectoryPath());
                if (outputFile.exists()) {
                    System.out.println("Output file already exists.");
                }
                else {
                    alsModel.save(config.getOutputDirectoryPath());
                }
                timer.stop();
                System.out.printf("Elapsed time: %s ms\n", timer.getElapsedTime());
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    private static void RunSparkMode(AlsToolConfig config) {
        String master = config.getSparkMaster();
        SparkConf conf = new SparkConf().setAppName("YSDA BigData ALS Tool").setMaster(master);
        JavaSparkContext context = new JavaSparkContext(conf);

        StopWatch timer = new StopWatch();
        timer.start();
        final String lineSeparator = config.getLineSeparator();
        JavaRDD<String> lines = context.textFile(config.getInputFilePath());
        JavaRDD<RatingDataRecord> records = lines.map(new Function<String, RatingDataRecord>() {
            @Override
            public RatingDataRecord call(String line) throws Exception {
                String[] parts = line.split(lineSeparator);
                RatingDataRecord result = new RatingDataRecord();
                result.user = parts[0];
                result.item = parts[1];
                result.rating = Double.parseDouble(parts[2]);
                return result;
            }
        });

        SparkAlsInitConfig alsConfig = new SparkAlsInitConfig();
        alsConfig.numFactors = config.getNumFactors();
        alsConfig.regCoefficient = config.getRegCoefficient();
        alsConfig.records = records;
        alsConfig.sparkContext = context;

        FactorizationError errorCounter = new FactorizationError();
        IAlsModel alsModel = new SparkAlsModel();
        alsModel.init(alsConfig);

        StopWatch iterTimer = new StopWatch();
        for (int iter = 1; iter <= 10; iter++) {
            iterTimer.start();
            alsModel.train(1);
            iterTimer.stop();
            double error = errorCounter.computeMSE(alsConfig.records, (BaseAlsModel) alsModel);
            System.out.printf("Iteration %s\n", iter);
            System.out.printf("MSE error %s\n", error);
            System.out.printf("Iteration time %s\n", iterTimer.getElapsedTime());
        }

        File outputFile = new File(config.getOutputDirectoryPath());
        if (outputFile.exists()) {
            System.out.println("Output file already exists.");
        }
        else {
            try {
                alsModel.save(config.getOutputDirectoryPath());
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }

        timer.stop();
        System.out.printf("Elapsed time: %s ms\n", timer.getElapsedTime());
    }
}
