package com.ysda.bigdata.preprocess;

import com.ysda.bigdata.utils.AlsToolConfig;

/**
 * Created by kwetril on 4/13/16.
 */
public class DataToMatrixConverterFactory {
    public static IDataToMatrixFileConverter getConverter(AlsToolConfig config) {
        return new DataToMatrixLocalFileConverter();
    }
}
