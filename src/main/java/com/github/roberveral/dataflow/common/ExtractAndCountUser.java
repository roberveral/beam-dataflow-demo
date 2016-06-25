package com.github.roberveral.dataflow.common;

import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

/**
 * Extracts the username from each {@link GithubEvent} and performs a count of events for each username.
 *
 * @author Roberto Veral
 */
public class ExtractAndCountUser extends PTransform<PCollection<GithubEvent>, PCollection<KV<String, Integer>>> {

    private static final long serialVersionUID = 1L;

    @Override
    public PCollection<KV<String, Integer>> apply(PCollection<GithubEvent> events) {
        /*
		 * Every event is mapped to a key-value username-1, and then is aggregated by key
		 * with an adding function for the values.
		 */
        return events.apply(MapElements.via((GithubEvent event) -> KV.of(event.getActor_login(), 1))
                .withOutputType(new TypeDescriptor<KV<String, Integer>>() {
                    private static final long serialVersionUID = 1L;
                })).apply(Sum.integersPerKey());
    }
}