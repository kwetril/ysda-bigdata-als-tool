package com.ysda.bigdata;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by xakl on 12.04.2016.
 */
public class AlsTool {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        try {
            StopWatch timer = new StopWatch();
            timer.start();
            FastScanner scanner = new FastScanner(args[0]);
            long wordCount = 0;
            String word = scanner.next();
            while (word != null) {
                wordCount++;
                word = scanner.next();
            }
            timer.stop();
            System.out.printf("Word count: %s\n", wordCount);
            System.out.printf("Elapsed time: %s ms\n", timer.getElapsedTime());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
