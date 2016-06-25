package com.github.roberveral.dataflow.common;

import org.json.JSONObject;

import com.google.cloud.dataflow.sdk.transforms.DoFn;

/**
 * Function {@link DoFn} that receives an event in JSON format and
 * parses it into a {@link GithubEvent} object.
 *
 * @author Roberto Veral
 */
public class ParseEventFn extends DoFn<String, GithubEvent> {

    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(ProcessContext processContext) throws Exception {
        /* Each line received is a JSON, which is parsed */
        JSONObject elemento = new JSONObject(processContext.element());
        /* Obtains the required fields from the JSON */
        String id = elemento.get("id").toString();
        String type = elemento.get("type").toString();
        String actor_id = elemento.getJSONObject("actor").get("id").toString();
        String actor_login = elemento.getJSONObject("actor").get("login").toString();
        String repo_id = elemento.getJSONObject("repo").get("id").toString();
        String repo_name = elemento.getJSONObject("repo").get("name").toString();
        /* Generates and output object */
        GithubEvent event = new GithubEvent(id, type, actor_id, actor_login, repo_id, repo_name);
        /* The object is returned from the function to a new PCollection */
        processContext.output(event);
    }
}