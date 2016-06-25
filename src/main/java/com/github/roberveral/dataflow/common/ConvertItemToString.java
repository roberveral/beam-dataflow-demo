package com.github.roberveral.dataflow.common;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

/**
 * Converts an item in the shape of (Key (FilteredCount, TotalCount)) to String
 *
 * @author Roberto Veral
 */
public class ConvertItemToString extends DoFn<KV<String, KV<Integer, Long>>, String> {
    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(DoFn<KV<String, KV<Integer, Long>>, String>.ProcessContext arg0)
            throws Exception {
        String s = arg0.element().getKey();
        /* The output is generated in CSV format: key;filteredCount;totalCount */
        arg0.output(s + ";" + arg0.element().getValue().getKey() + ";" + arg0.element().getValue().getValue());
    }

}
