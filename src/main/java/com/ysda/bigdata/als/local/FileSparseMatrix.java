package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.ISparseMatrix;
import com.ysda.bigdata.als.SparseRow;
import com.ysda.bigdata.utils.FastScanner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Exchanger;

/**
 * Created by xakl on 13.04.2016.
 */
public class FileSparseMatrix implements ISparseMatrix {

    private String matrixFilePath;
    private int numRows;
    private int numCols;

    public FileSparseMatrix(String matrixFilePath) throws IOException {
        this.matrixFilePath = matrixFilePath;
        FastScanner scanner = new FastScanner(matrixFilePath);
        String[] rowsColsPair = scanner.nextLine().split(" ");
        numRows = Integer.parseInt(rowsColsPair[0]);
        numCols = Integer.parseInt(rowsColsPair[1]);
    }

    @Override
    public int getNumRows() {
        return numRows;
    }

    @Override
    public int getNumCols() {
        return numCols;
    }

    class MatrixFileIterator implements Iterator<SparseRow>
    {
        private FastScanner scanner;
        private SparseRow nextSparseRow;

        private MatrixFileIterator(String filePath) throws IOException {
            scanner = new FastScanner(filePath);
            scanner.nextLine();
            nextSparseRow = scanner.nextSparseRow();
        }

        @Override
        public boolean hasNext() {
            return nextSparseRow != null;
        }

        @Override
        public SparseRow next() {
            SparseRow result = nextSparseRow;
            try {
                nextSparseRow = scanner.nextSparseRow();
            } catch (Exception ex) {
                ex.printStackTrace();
                nextSparseRow = null;
            }
            return result;
        }

        @Override
        public void remove() {
        }
    }

    @Override
    public Iterator<SparseRow> iterator() {
        try {
            return new MatrixFileIterator(matrixFilePath);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
