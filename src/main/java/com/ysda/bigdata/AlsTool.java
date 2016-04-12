package com.ysda.bigdata;

import com.ysda.bigdata.preprocess.DataToMatrixConverterFactory;
import com.ysda.bigdata.preprocess.IDataToMatrixFileConverter;
import com.ysda.bigdata.utils.AlsToolConfig;
import com.ysda.bigdata.utils.FastScanner;
import com.ysda.bigdata.utils.StopWatch;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.FileNotFoundException;
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
        }

        try {
            if (CheckDatasetGrouppedByRow(config)) {
                System.out.println("Dataset is grouped by rows.");
            } else {
                System.out.println("Dataset is not grouped by rows.");
                return;
            }

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

    private static boolean CheckDatasetGrouppedByRow(AlsToolConfig config) throws IOException {
        HashSet<String> uniqueRows = new HashSet<String>();
        FastScanner scanner = new FastScanner(config.getInputFilePath());
        String line = scanner.nextLine();
        String currentRow = null;
        while (line != null) {
            String newRow = line.split(" ")[0];
            if (newRow != currentRow) {
                if (uniqueRows.contains(newRow)) {
                    return false;
                }
                uniqueRows.add(newRow);
            }
            line = scanner.nextLine();
        }
        return true;
    }
}
