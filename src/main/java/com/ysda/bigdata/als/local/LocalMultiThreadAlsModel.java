package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.*;
import com.ysda.bigdata.utils.FastScanner;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.*;

/**
 * Created by kwetril on 4/13/16.
 */
public class LocalMultiThreadAlsModel extends BaseAlsModel {
    private ISparseMatrix ratingMatrix;
    private ISparseMatrix transposedRatingMatrix;
    private ThreadPoolExecutor threadPool;
    private Semaphore semaphore;
    int queueSize;
    int numThreads;

    public void init(BaseAlsInitConfig config) {
        super.init(config);
        LocalMultiThreadAlsInitConfig localConfig = (LocalMultiThreadAlsInitConfig) config;
        this.ratingMatrix = localConfig.ratingMatrix;
        this.transposedRatingMatrix = localConfig.transposedRatingMatrix;
        this.numThreads = localConfig.numThreads;
        this.queueSize = 2 * numThreads;
    }

    @Override
    public void train(int numIterations) {
        BlockingQueue queue = new ArrayBlockingQueue(queueSize);
        this.threadPool = new ThreadPoolExecutor(numThreads, numThreads, 1, TimeUnit.DAYS, queue);
        this.semaphore = new Semaphore(queueSize);

        for (int i = 0; i < numIterations; i++) {
            doIteration();
        }

        this.threadPool.shutdown();
        try {
            this.threadPool.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void batchPredicition(String inputPath, String outputPath, String lineSeparator) throws IOException {
        FastScanner scanner = new FastScanner(inputPath);
        String line = scanner.nextLine();
        try (FileWriter output = new FileWriter(outputPath)) {
            while (line != null) {
                String[] parts = line.split(lineSeparator);
                double rating = predict(parts[0], parts[1]);
                output.write(String.format("%s\t%s\t%s\n", parts[0], parts[1], rating));
            }
        }
    }

    private void doIteration() {
        for (SparseRow ratingsRow : ratingMatrix) {
            try {
                semaphore.acquire();
                threadPool.execute(new ComputeRowFromOptimizationJob(semaphore, ratingsRow,
                        rowFactorsMatrix, colFactorsMatrix));
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }
        }
        try {
            semaphore.acquire(queueSize);
            semaphore.release(queueSize);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (SparseRow ratingsRow : transposedRatingMatrix) {
            try {
                semaphore.acquire();
                threadPool.execute(new ComputeRowFromOptimizationJob(semaphore, ratingsRow,
                        colFactorsMatrix, rowFactorsMatrix));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            semaphore.acquire(queueSize);
            semaphore.release(queueSize);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class ComputeRowFromOptimizationJob implements Runnable {
        private SparseRow ratingsRow;
        private FactorMatrix matrixToOptimize;
        private FactorMatrix factorMatrix;
        private final Semaphore semaphore;

        ComputeRowFromOptimizationJob(Semaphore semaphore, SparseRow ratingsRow,
                                      FactorMatrix matrixToOptimize, FactorMatrix factorMatrix) {
            this.ratingsRow = ratingsRow;
            this.matrixToOptimize = matrixToOptimize;
            this.factorMatrix = factorMatrix;
            this.semaphore = semaphore;
        }

        @Override
        public void run() {
            try {
                DenseMatrix rowFactorsSubmatrix = factorMatrix.getSubmatrix(ratingsRow.getColIndices());
                DenseMatrix transposedRowFactorsSubmatrix = rowFactorsSubmatrix.transpose();
                double[] result = transposedRowFactorsSubmatrix
                        .multiply(rowFactorsSubmatrix)
                        .addDiag(regCoefficient)
                        .inverse()
                        .multiply(transposedRowFactorsSubmatrix)
                        .multiply(ratingsRow.getValues());
                matrixToOptimize.setRow(ratingsRow.getRowIndex(), result);
            }
            finally {
                semaphore.release();
            }
        }
    }
}
