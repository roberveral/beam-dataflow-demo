/**
 *
 */
package com.github.roberveral.dataflow.transforms.join;

import com.github.roberveral.dataflow.transforms.CoGroupByKeyFactory;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Preconditions;

/**
 * Utility class with different versions of joins. All methods join two collections of
 * key/value pairs (KV).
 * <p>
 * Taken from https://github.com/GoogleCloudPlatform/DataflowJavaSDK/tree/master/contrib/join-library
 * and modified to make use of the CoGroupByKeyFactory class, making it work with the SparkPipelineRunner.
 * <p>
 * The original package is org.linuxalert.dataflow.
 * All rights reserved to its authors:
 * <p>
 * Google Inc.
 * Magnus Runesson, M.Runesson [at] gmail [dot] com
 */
public class Join {

    /**
     * Inner join of two collections of KV elements.
     *
     * @param leftCollection  Left side collection to join.
     * @param rightCollection Right side collection to join.
     * @param <K>             Type of the key for both collections
     * @param <V1>            Type of the values for the left collection.
     * @param <V2>            Type of the values for the right collection.
     * @return A joined collection of KV where Key is the key and value is a
     * KV where Key is of type V1 and Value is type V2.
     */
    public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> innerJoin(
            final PCollection<KV<K, V1>> leftCollection, final PCollection<KV<K, V2>> rightCollection, String runner) {
        Preconditions.checkNotNull(leftCollection);
        Preconditions.checkNotNull(rightCollection);

        final TupleTag<V1> v1Tuple = new TupleTag<>();
        final TupleTag<V2> v2Tuple = new TupleTag<>();

        PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
                KeyedPCollectionTuple.of(v1Tuple, leftCollection)
                        .and(v2Tuple, rightCollection)
                        .apply(CoGroupByKeyFactory.<K>obtain(runner));

        return coGbkResultCollection.apply(ParDo.of(
                new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @Override
                    public void processElement(ProcessContext c) {
                        KV<K, CoGbkResult> e = c.element();

                        Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                        Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

                        for (V1 leftValue : leftValuesIterable) {
                            for (V2 rightValue : rightValuesIterable) {
                                c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                            }
                        }
                    }
                }))
                .setCoder(KvCoder.of(((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                        KvCoder.of(((KvCoder) leftCollection.getCoder()).getValueCoder(),
                                ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }

    /**
     * Left Outer Join of two collections of KV elements.
     *
     * @param leftCollection  Left side collection to join.
     * @param rightCollection Right side collection to join.
     * @param nullValue       Value to use as null value when right side do not match left side.
     * @param <K>             Type of the key for both collections
     * @param <V1>            Type of the values for the left collection.
     * @param <V2>            Type of the values for the right collection.
     * @return A joined collection of KV where Key is the key and value is a
     * KV where Key is of type V1 and Value is type V2. Values that
     * should be null or empty is replaced with nullValue.
     */
    public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> leftOuterJoin(
            final PCollection<KV<K, V1>> leftCollection,
            final PCollection<KV<K, V2>> rightCollection,
            final V2 nullValue, String runner) {
        Preconditions.checkNotNull(leftCollection);
        Preconditions.checkNotNull(rightCollection);
        Preconditions.checkNotNull(nullValue);

        final TupleTag<V1> v1Tuple = new TupleTag<>();
        final TupleTag<V2> v2Tuple = new TupleTag<>();

        PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
                KeyedPCollectionTuple.of(v1Tuple, leftCollection)
                        .and(v2Tuple, rightCollection)
                        .apply(CoGroupByKeyFactory.<K>obtain(runner));

        return coGbkResultCollection.apply(ParDo.of(
                new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @Override
                    public void processElement(ProcessContext c) {
                        KV<K, CoGbkResult> e = c.element();

                        Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                        Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

                        for (V1 leftValue : leftValuesIterable) {
                            if (rightValuesIterable.iterator().hasNext()) {
                                for (V2 rightValue : rightValuesIterable) {
                                    c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                                }
                            } else {
                                c.output(KV.of(e.getKey(), KV.of(leftValue, nullValue)));
                            }
                        }
                    }
                }))
                .setCoder(KvCoder.of(((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                        KvCoder.of(((KvCoder) leftCollection.getCoder()).getValueCoder(),
                                ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }

    /**
     * Right Outer Join of two collections of KV elements.
     *
     * @param leftCollection  Left side collection to join.
     * @param rightCollection Right side collection to join.
     * @param nullValue       Value to use as null value when left side do not match right side.
     * @param <K>             Type of the key for both collections
     * @param <V1>            Type of the values for the left collection.
     * @param <V2>            Type of the values for the right collection.
     * @return A joined collection of KV where Key is the key and value is a
     * KV where Key is of type V1 and Value is type V2. Keys that
     * should be null or empty is replaced with nullValue.
     */
    public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> rightOuterJoin(
            final PCollection<KV<K, V1>> leftCollection,
            final PCollection<KV<K, V2>> rightCollection,
            final V1 nullValue, String runner) {
        Preconditions.checkNotNull(leftCollection);
        Preconditions.checkNotNull(rightCollection);
        Preconditions.checkNotNull(nullValue);

        final TupleTag<V1> v1Tuple = new TupleTag<>();
        final TupleTag<V2> v2Tuple = new TupleTag<>();

        PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
                KeyedPCollectionTuple.of(v1Tuple, leftCollection)
                        .and(v2Tuple, rightCollection)
                        .apply(CoGroupByKeyFactory.<K>obtain(runner));

        return coGbkResultCollection.apply(ParDo.of(
                new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @Override
                    public void processElement(ProcessContext c) {
                        KV<K, CoGbkResult> e = c.element();

                        Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                        Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

                        for (V2 rightValue : rightValuesIterable) {
                            if (leftValuesIterable.iterator().hasNext()) {
                                for (V1 leftValue : leftValuesIterable) {
                                    c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                                }
                            } else {
                                c.output(KV.of(e.getKey(), KV.of(nullValue, rightValue)));
                            }
                        }
                    }
                }))
                .setCoder(KvCoder.of(((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                        KvCoder.of(((KvCoder) leftCollection.getCoder()).getValueCoder(),
                                ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }
}
