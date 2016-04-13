package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by kwetril on 4/13/16.
 */
public class LocalMultiThreadAlsAlgorithm implements IAlsAlgorithm {
    private ISparseMatrix ratingMatrix;
    private ISparseMatrix transposedRatingMatrix;
    private IDenseMatrix rowFactorsMatrix;
    private IDenseMatrix colFactorsMatrix;
    private int numFactors;
    private double regCoefficient;
    private ThreadPoolExecutor threadPool;
    private Semaphore semaphore;
    int queueSize;
    int numThreads;

    public LocalMultiThreadAlsAlgorithm(int numThreads) {
        this.numThreads = numThreads;
        this.queueSize = 2 * numThreads;

    }

    @Override
    public void init(ISparseMatrix ratingMatrix, ISparseMatrix transposedRatingMatrix,
                     int numFactors, double regCoefficient) {
        this.ratingMatrix = ratingMatrix;
        this.transposedRatingMatrix = transposedRatingMatrix;
        this.numFactors = numFactors;
        this.regCoefficient = regCoefficient;
        this.rowFactorsMatrix = new DenseMatrix(ratingMatrix.getNumRows(), numFactors);
        this.colFactorsMatrix = new DenseMatrix(transposedRatingMatrix.getNumRows(), numFactors);
    }

    @Override
    public MatrixFactorizationResult doIterations(int numIterations) {
        BlockingQueue queue = new ArrayBlockingQueue(queueSize);
        this.threadPool = new ThreadPoolExecutor(numThreads, numThreads, 1, TimeUnit.DAYS, queue);
        this.semaphore = new Semaphore(queueSize);

        MatrixFactorizationResult result = null;
        for (int i = 0; i < numIterations; i++) {
            result = doIteration();
        }

        this.threadPool.shutdown();
        try {
            this.threadPool.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return result;
    }

    private MatrixFactorizationResult doIteration() {
        for (SparseRow ratingsRow : ratingMatrix) {
            try {
                semaphore.acquire();
                threadPool.execute(new ComputeRowFromOptimizationJob(semaphore, ratingsRow,
                        rowFactorsMatrix, colFactorsMatrix));
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
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

        MatrixFactorizationResult result = new MatrixFactorizationResult();
        result.rowFactorsMatrix = rowFactorsMatrix;
        result.colFactorsMatrix = colFactorsMatrix;
        return result;
    }

    private class ComputeRowFromOptimizationJob implements Runnable {
        private SparseRow ratingsRow;
        private IDenseMatrix matrixToOptimize;
        private IDenseMatrix factorMatrix;
        private final Semaphore semaphore;

        ComputeRowFromOptimizationJob(Semaphore semaphore, SparseRow ratingsRow,
                                      IDenseMatrix matrixToOptimize, IDenseMatrix factorMatrix) {
            this.ratingsRow = ratingsRow;
            this.matrixToOptimize = matrixToOptimize;
            this.factorMatrix = factorMatrix;
            this.semaphore = semaphore;
        }

        @Override
        public void run() {
            try {
                IDenseMatrix rowFactorsSubmatrix = factorMatrix.getSubmatrix(ratingsRow.getColIndices());
                IDenseMatrix transposedRowFactorsSubmatrix = rowFactorsSubmatrix.transpose();
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
