package com.github.roberveral.dataflow.transforms;

import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKeyOnly;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * Obtains the correct CoGroupByKey for the given {@link PipelineRunner} name,
 * in terms of compatibility issues.
 *
 * @author Roberto Veral
 */
public class CoGroupByKeyFactory {
    /**
     * Obtains the correct {@link CoGroupByKey} transform for the given {@link PipelineRunner} name.
     *
     * @param runner {@link PipelineRunner} name.
     * @return {@link CoGroupByKey} transform.
     */
    public static <K> PTransform<KeyedPCollectionTuple<K>, PCollection<KV<K, CoGbkResult>>> obtain(String runner) {
        /* If the runner is SparkPipelineRunner, the CoGroupByKeyOnly transformation must be used */
        if (runner.contains("Spark")) {
            return CoGroupByKeyOnly.<K>create();
        } else {
			/* Otherwise, the usual CoGroupByKey is returned */
            return CoGroupByKey.<K>create();
        }
    }
}
