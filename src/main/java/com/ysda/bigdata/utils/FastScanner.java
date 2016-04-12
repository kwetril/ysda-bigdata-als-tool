package com.ysda.bigdata.utils;

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
        return  bufferedReader.readLine();
    }

    public RatingDataRecord nextRatingDataRecord(String splitRegex) throws IOException {
        String line = bufferedReader.readLine();
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

    public String next() throws IOException {
        while (tokenizer == null || !tokenizer.hasMoreElements()) {
            String line = bufferedReader.readLine();
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
