package com.github.roberveral.dataflow;

import com.github.roberveral.dataflow.common.ComparadorJoinKV;
import com.github.roberveral.dataflow.common.ConvertItemToString;
import com.github.roberveral.dataflow.common.GithubEvent;
import com.github.roberveral.dataflow.common.GithubOptions;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Top;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.github.roberveral.dataflow.common.ExtractAndCountUser;
import com.github.roberveral.dataflow.common.ExtraeUserName;
import com.github.roberveral.dataflow.common.GithubEventFilter;
import com.github.roberveral.dataflow.common.ParseEventFn;
import com.github.roberveral.dataflow.transforms.PipelineRunnerFactory;
import com.github.roberveral.dataflow.transforms.join.Join;

/**
 * Test workflow that reads historical Github data and generates an output with the top 100
 * users in number of contributions given the request type filter, with their total contribution count.
 * <p>
 * Example usage:
 * spark-submit --class com.github.roberveral.dataflow.GithubAnalisisJoin target/BeamDemo-bundled-0.0.1-SNAPSHOT.jar
 *              --runnerName=com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner
 *              --project=<YOUR_PROJECT>
 *              --stagingLocation=gs://<YOUR_BUCKET>
 *              --inputFile=gs://<YOUR_BUCKET>/github_data/*.json
 *              --output=gs://<YOUR_BUCKET>/output/gittop100
 *              --eventSource='s'
 *              --eventType='e'
 *              --numWorkers=6
 *              --workerMachineType="n1-standard-8"
 *              --zone="europe-west1-d"
 *              --sparkMaster=yarn-client
 *
 * @author Roberto Veral
 */
public class GithubAnalisisJoin {
    public static void main(String[] args) throws ClassNotFoundException {
        // The input arguments define the runner which is gonna execute the program
        GithubOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(GithubOptions.class);
        options.setRunner(PipelineRunnerFactory.obtain(options.getRunnerName()));

		/* The filters are taken from the arguments */
        final String repositorio = options.getEventSource();
        final String tipo = options.getEventType();

        // Pipeline creation, the data flow that is gonna be performed
        Pipeline p = Pipeline.create(options);

		/* STEP 1: Reading and parsing JSON files from Cloud Storage */
        PCollection<GithubEvent> eventos = p
                .apply(TextIO.Read.named("Lectura del fichero").from(options.getInputFile()))
                .apply(ParDo.named("JSONParse").of(new ParseEventFn()));
		/* STEP 2: Two different flows */
		/* STEP 2 FLOW 1: obtain users' activity with the given filters */
        PCollection<KV<String, Integer>> topUsersRepo = eventos
                .apply("Filtro de repositorio y evento", Filter.byPredicate(new GithubEventFilter(repositorio, tipo)))
                .apply("Agregacion por usuario", new ExtractAndCountUser());
		/* STEP 2 FLOW 2: obtain total users' contributions without any filter */
        PCollection<KV<String, Long>> usersGlobal = eventos
                .apply("Obtencion de los nombres", MapElements.via(new ExtraeUserName()))
                .apply("Conteo global por usuario", Count.perElement());

		/* STEP 3: both flows are joined and the result is converted to a printable string */
        Join.innerJoin(topUsersRepo, usersGlobal, options.getRunnerName())
                .apply("Obtencion del top 100", Top.of(100, new ComparadorJoinKV()))
                .apply("Distribucion de los resultados", Flatten.iterables())
                .apply("Conversion a String", ParDo.of(new ConvertItemToString()))
                .apply(TextIO.Write.named("Imprimir resultados").to(options.getOutput()).withoutSharding());

        // Pipeline execution
        p.run();
    }
}
