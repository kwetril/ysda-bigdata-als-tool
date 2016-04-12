package com.ysda.bigdata;

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

    String next() throws IOException {
        while (tokenizer == null || !tokenizer.hasMoreElements()) {
            String line = bufferedReader.readLine();
            if (line == null) {
                return null;
            }
            tokenizer = new StringTokenizer(line);
        }
        return tokenizer.nextToken();
    }

    int nextInt() throws IOException {
        return Integer.parseInt(next());
    }

    long nextLong() throws IOException {
        return Long.parseLong(next());
    }

    double nextDouble() throws IOException {
        return Double.parseDouble(next());
    }
}
