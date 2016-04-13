package com.ysda.bigdata.utils;

import com.ysda.bigdata.als.SparseRow;

import java.io.*;
import java.util.StringTokenizer;

/**
 * Created by xakl on 12.04.2016.
 */
public class FastScanner {
    BufferedReader bufferedReader;
    StringTokenizer tokenizer;

    public FastScanner(String filePath) throws FileNotFoundException {
        bufferedReader = new BufferedReader(new FileReader(filePath));
    }

    public FastScanner() {
        bufferedReader = new BufferedReader(new InputStreamReader(System.in));
    }

    public String nextLine() throws IOException {
        String result = bufferedReader.readLine();
        if (result == null) {
            bufferedReader.close();
        }
        return result;
    }

    public RatingDataRecord nextRatingDataRecord(String splitRegex) throws IOException {
        String line = nextLine();
        if (line == null) {
            return  null;
        }
        String[] lineElements = line.split(splitRegex);
        RatingDataRecord result = new RatingDataRecord();
        result.rowId = lineElements[0];
        result.columnId = lineElements[1];
        result.rating = Double.parseDouble(lineElements[2]);
        return result;
    }

    public SparseRow nextSparseRow() throws IOException {
        String line = nextLine();
        if (line == null) {
            return null;
        }
        String[] lineElements = line.split(" ");
        int rowIndex = Integer.parseInt(lineElements[0]);
        int[] indices = new int[lineElements.length - 1];
        double[] values = new double[lineElements.length - 1];
        for (int i = 1; i < lineElements.length; i++) {
            String[] indexValuePair = lineElements[i].split(":");
            indices[i - 1] = Integer.parseInt(indexValuePair[0]);
            values[i - 1] = Double.parseDouble(indexValuePair[1]);
        }
        return new SparseRow(rowIndex, indices, values);
    }

    public String next() throws IOException {
        while (tokenizer == null || !tokenizer.hasMoreElements()) {
            String line = nextLine();
            if (line == null) {
                return null;
            }
            tokenizer = new StringTokenizer(line);
        }
        return tokenizer.nextToken();
    }

    public int nextInt() throws IOException {
        return Integer.parseInt(next());
    }

    public long nextLong() throws IOException {
        return Long.parseLong(next());
    }

    public double nextDouble() throws IOException {
        return Double.parseDouble(next());
    }
}
