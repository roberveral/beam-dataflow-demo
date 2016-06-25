package com.github.roberveral.dataflow.common;

import com.google.cloud.dataflow.sdk.transforms.SerializableComparator;
import com.google.cloud.dataflow.sdk.values.KV;

/**
 * Comparator for objects of type KV(String, KV(Integer, Long)).
 * It makes the comparison by the value of the value (a Long data).
 *
 * @author Roberto Veral
 */
public class ComparadorJoinKV implements SerializableComparator<KV<String, KV<Integer, Long>>> {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(KV<String, KV<Integer, Long>> o1, KV<String, KV<Integer, Long>> o2) {
        return Long.compare(o1.getValue().getKey(), o2.getValue().getValue());
    }
}