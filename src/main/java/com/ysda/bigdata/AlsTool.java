package com.ysda.bigdata;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.FileNotFoundException;
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

        System.out.println("Hello world!");
        try {
            StopWatch timer = new StopWatch();
            timer.start();
            FastScanner scanner = new FastScanner(config.getInputFilePath());
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
