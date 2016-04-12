package com.ysda.bigdata.preprocess;

import com.ysda.bigdata.utils.AlsToolConfig;
import com.ysda.bigdata.utils.FastScanner;
import com.ysda.bigdata.utils.RatingDataRecord;

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
        FastScanner scanner;
        try {
            scanner = new FastScanner(config.getInputFilePath());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Input file not found.");
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
        String rowsValue2IdFilePath = new File(outputFolder, "rows-value2id.dat").getAbsolutePath();
        String colsValue2IdFilePath = new File(outputFolder, "cols-value2id.dat").getAbsolutePath();

        HashMap<String, Integer> rowValueIdMap = new HashMap<>();
        HashMap<String, Integer> colValueIdMap = new HashMap<>();
        int nextRowId = 0;
        int nextColId = 0;
        int curRowId, curColId;
        String separator = config.getLineSeparator();

        try (FileWriter matrixFile = new FileWriter(matrixFilePath)) {
            RatingDataRecord ratingRecord = scanner.nextRatingDataRecord(separator);
            while (ratingRecord != null) {
                if (rowValueIdMap.containsKey(ratingRecord.rowId)) {
                    curRowId = rowValueIdMap.get(ratingRecord.rowId);
                } else {
                    rowValueIdMap.put(ratingRecord.rowId, nextRowId);
                    curRowId = nextRowId;
                    nextRowId++;
                }

                if (colValueIdMap.containsKey(ratingRecord.columnId)) {
                    curColId = colValueIdMap.get(ratingRecord.columnId);
                } else {
                    colValueIdMap.put(ratingRecord.columnId, nextColId);
                    curColId = nextColId;
                    nextColId++;
                }

                matrixFile.write(String.format("%s %s %s\n", curRowId, curColId, ratingRecord.rating));
                ratingRecord = scanner.nextRatingDataRecord(separator);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        try (FileWriter rowsValue2IdFile = new FileWriter(rowsValue2IdFilePath)) {
            for(Map.Entry<String, Integer> item : rowValueIdMap.entrySet()) {
                rowsValue2IdFile.write(String.format("%s %s\n", item.getKey(), item.getValue()));
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        try (FileWriter colsValue2IdFile = new FileWriter(colsValue2IdFilePath)) {
            for(Map.Entry<String, Integer> item : colValueIdMap.entrySet()) {
                colsValue2IdFile.write(String.format("%s %s\n", item.getKey(), item.getValue()));
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
    }
}
