package com.ysda.bigdata;

import com.ysda.bigdata.als.IAlsAlgorithm;
import com.ysda.bigdata.als.ISparseMatrix;
import com.ysda.bigdata.als.MatrixFactorizationResult;
import com.ysda.bigdata.als.local.FactorizationError;
import com.ysda.bigdata.als.local.FileSparseMatrix;
import com.ysda.bigdata.als.local.LocalAlsAlgorithm;
import com.ysda.bigdata.als.local.LocalMultiThreadAlsAlgorithm;
import com.ysda.bigdata.preprocess.DataToMatrixConverterFactory;
import com.ysda.bigdata.preprocess.IDataToMatrixFileConverter;
import com.ysda.bigdata.utils.AlsToolConfig;
import com.ysda.bigdata.utils.DenseMatrixWriter;
import com.ysda.bigdata.utils.FastScanner;
import com.ysda.bigdata.utils.StopWatch;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

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

        if (config.getMode() == AlsToolConfig.ExecutionMode.PREPROCESSING) {
            IDataToMatrixFileConverter converter = DataToMatrixConverterFactory.getConverter(config);
            converter.doConversion(config);
            return;
        } else {
            try {
                ISparseMatrix ratingMatrix = null;
                StopWatch timer = new StopWatch();
                timer.start();
                ratingMatrix = new FileSparseMatrix(config.getInputFilePath());
                ISparseMatrix transposedRatingMatrix = new FileSparseMatrix(config.getTransposedInputFilePath());
                int numFactors = config.getNumFactors();
                double regCoefficient = config.getRegCoefficient();
                //IAlsAlgorithm alsAlgorithm = new LocalAlsAlgorithm();
                IAlsAlgorithm alsAlgorithm = new LocalMultiThreadAlsAlgorithm(4);
                alsAlgorithm.init(ratingMatrix, transposedRatingMatrix, numFactors, regCoefficient);
                FactorizationError errorCounter = new FactorizationError();
                MatrixFactorizationResult result = null;
                StopWatch iterTimer = new StopWatch();
                for (int iter = 1; iter <= 10; iter++) {
                    iterTimer.start();
                    result = alsAlgorithm.doIterations(1);
                    iterTimer.stop();
                    double error = errorCounter.computeMSE(ratingMatrix,
                            result.rowFactorsMatrix, result.colFactorsMatrix);
                    System.out.printf("Iteration %s\n", iter);
                    System.out.printf("MSE error %s\n", error);
                    System.out.printf("Iteration time %s\n", iterTimer.getElapsedTime());
                }

                File outputFolder = new File(config.getOutputDirectoryPath());
                if (outputFolder.exists()) {
                    System.out.println("Output directory already exists.");
                    return;
                }

                if(!outputFolder.mkdir()) {
                    System.out.println("Output directory was not created.");
                    return;
                }

                String rowFactorsFilePath = new File(outputFolder, "row-factors.dat").getAbsolutePath();
                String colFactorsFilePath = new File(outputFolder, "col-factors.dat").getAbsolutePath();
                DenseMatrixWriter matrixWriter = new DenseMatrixWriter();
                matrixWriter.writeMatrix(rowFactorsFilePath, result.rowFactorsMatrix);
                matrixWriter.writeMatrix(colFactorsFilePath, result.colFactorsMatrix);
                timer.stop();
                System.out.printf("Elapsed time: %s ms", timer.getElapsedTime());
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
    }
}
