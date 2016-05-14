package com.ysda.bigdata.preprocess;

import com.ysda.bigdata.als.local.ISparseMatrix;
import com.ysda.bigdata.als.local.MemorySparseMatrixBuilder;
import com.ysda.bigdata.als.local.SparseMatrixWriter;
import com.ysda.bigdata.utils.AlsToolConfig;
import com.ysda.bigdata.utils.FastScanner;
import com.ysda.bigdata.utils.RatingDataRecord;
import com.ysda.bigdata.utils.StopWatch;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kwetril on 4/12/16.
 */
public class DataToMatrixLocalFileConverter implements IDataToMatrixFileConverter {
    public void doConversion(AlsToolConfig config) {
        StopWatch timer = new StopWatch();
        timer.start();
        FastScanner scanner;
        MemorySparseMatrixBuilder ratingMatrixBuilder = new MemorySparseMatrixBuilder();
        MemorySparseMatrixBuilder transposedRatingMatrixBuilder = new MemorySparseMatrixBuilder();
        try {
            scanner = new FastScanner(config.getInputFilePath());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Input file not found.");
            return;
        }

        String separator = config.getLineSeparator();
        try {
            RatingDataRecord ratingRecord = scanner.nextRatingDataRecord(separator);
            while (ratingRecord != null) {
                ratingMatrixBuilder.addElement(ratingRecord.user, ratingRecord.item, ratingRecord.rating);
                transposedRatingMatrixBuilder.addElement(ratingRecord.item, ratingRecord.user, ratingRecord.rating);
                ratingRecord = scanner.nextRatingDataRecord(separator);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
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

        String matrixFilePath = new File(outputFolder, "matrix.dat").getAbsolutePath();
        String transposedMatrixFilePath = new File(outputFolder, "transposed-matrix.dat").getAbsolutePath();

        SparseMatrixWriter sparseMatrixWriter = new SparseMatrixWriter();
        ISparseMatrix ratingMatrix = ratingMatrixBuilder.build();
        try {
            sparseMatrixWriter.writeMatrix(matrixFilePath, ratingMatrix);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        ISparseMatrix transposedRatingMatrix = transposedRatingMatrixBuilder.build();
        try {
            sparseMatrixWriter.writeMatrix(transposedMatrixFilePath, transposedRatingMatrix);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        timer.stop();
        System.out.printf("Elapsed time: %s ms\n", timer.getElapsedTime());
    }
}
