package com.ysda.bigdata;

import com.ysda.bigdata.als.IAlsModel;
import com.ysda.bigdata.als.local.ISparseMatrix;
import com.ysda.bigdata.als.local.*;
import com.ysda.bigdata.preprocess.DataToMatrixConverterFactory;
import com.ysda.bigdata.preprocess.IDataToMatrixFileConverter;
import com.ysda.bigdata.utils.AlsToolConfig;
import com.ysda.bigdata.utils.RatingDataRecord;
import com.ysda.bigdata.utils.StopWatch;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
            IDataToMatrixFileConverter converter = DataToMatrixConverterFactory.getConverter(config);
            converter.doConversion(config);
            return;
        } else {
            try {
                StopWatch timer = new StopWatch();
                timer.start();
                LocalAslInitConfig alsConfig = new LocalAslInitConfig();
                alsConfig.ratingMatrix = new FileSparseMatrix(config.getInputFilePath());
                alsConfig.transposedRatingMatrix = new FileSparseMatrix(config.getTransposedInputFilePath());
                alsConfig.numFactors = config.getNumFactors();
                alsConfig.regCoefficient = config.getRegCoefficient();
                IAlsModel alsModel = new LocalAlsModel();
                //IAlsAlgorithm alsAlgorithm = new LocalMultiThreadAlsAlgorithm(4);
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

                File outputFolder = new File(config.getOutputDirectoryPath());
                if (outputFolder.exists()) {
                    System.out.println("Output directory already exists.");
                    return;
                }
                alsModel.save(config.getOutputDirectoryPath());
                timer.stop();
                System.out.printf("Elapsed time: %s ms", timer.getElapsedTime());
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    private static void RunSparkMode(AlsToolConfig config) {
        SparkConf conf = new SparkConf().setAppName("YSDA BigData ALS Tool").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        List<Integer> test = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            test.add(i);
        }
        JavaRDD<Integer> rdd = context.parallelize(test);
        int sum = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer acc, Integer value) throws Exception {
                return acc + value;
            }
        });
        System.out.println(sum);

        JavaRDD<String> inputLines = context.textFile(config.getInputFilePath());
        final String lineSeparator = config.getLineSeparator();
        JavaRDD<RatingDataRecord> ratingDataRecords = inputLines.map(new Function<String, RatingDataRecord>() {
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
    }
}
