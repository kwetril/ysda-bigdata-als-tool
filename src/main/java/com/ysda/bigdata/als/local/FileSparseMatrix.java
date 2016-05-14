package com.ysda.bigdata.als.local;

import com.ysda.bigdata.utils.FastScanner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by xakl on 13.04.2016.
 */
public class FileSparseMatrix implements ISparseMatrix {
    private String matrixFilePath;

    public FileSparseMatrix(String matrixFilePath) {
        this.matrixFilePath = matrixFilePath;
    }

    class MatrixFileIterator implements Iterator<SparseRow>
    {
        private FastScanner scanner;
        private SparseRow nextSparseRow;

        private MatrixFileIterator(String filePath) throws IOException {
            scanner = new FastScanner(filePath);
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
