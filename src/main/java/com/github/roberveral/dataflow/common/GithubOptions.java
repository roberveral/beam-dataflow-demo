package com.github.roberveral.dataflow.common;

import com.cloudera.dataflow.spark.SparkPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

/**
 * Interface that sets the expected command-line arguments for the program,
 * and also defines default values if missing
 *
 * @author Roberto Veral
 */
public interface GithubOptions extends PipelineOptions, SparkPipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("gs://<YOUR_BUCKET>/github_data/*.json")
    String getInputFile();

    void setInputFile(String value);

    @Description("Path of the file to write to")
    @Default.String("gs://<YOUT_BUCKET>/github_salida/git")
    String getOutput();

    void setOutput(String value);

    @Description("Runner to execute in")
    @Default.String("com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner")
    String getRunnerName();

    void setRunnerName(String value);

    @Description("Type of event")
    @Default.String("PullRequestEvent")
    String getEventType();

    void setEventType(String value);

    @Description("Source of event")
    @Default.String("Google")
    String getEventSource();

    void setEventSource(String value);
}
