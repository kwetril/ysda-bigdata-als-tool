package com.ysda.bigdata.als.local;

import com.ysda.bigdata.als.ISparseMatrix;
import com.ysda.bigdata.als.SparseRow;
import com.ysda.bigdata.utils.FastScanner;

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

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public SparseRow next() {
            return null;
        }

        @Override
        public void remove() {
        }
    }

    @Override
    public Iterator<SparseRow> iterator() {
        return null;
    }
}
