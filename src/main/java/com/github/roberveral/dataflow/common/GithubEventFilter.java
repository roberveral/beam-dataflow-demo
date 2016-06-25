package com.github.roberveral.dataflow.common;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;

/**
 * Applies a filter to a {@link GithubEvent} over the event type and the repository name.
 *
 * @author Roberto Veral
 */
public class GithubEventFilter implements SerializableFunction<GithubEvent, Boolean> {
    private static final long serialVersionUID = 1L;
    private String repositorio;
    private String tipo;

    /**
     * @param repositorio string to search in repository name
     * @param tipo        string to search in event type
     */
    public GithubEventFilter(String repositorio, String tipo) {
        this.repositorio = repositorio;
        this.tipo = tipo;
    }


    @Override
    public Boolean apply(GithubEvent githubEvent) {
        return githubEvent.getType().contains(tipo) && (githubEvent.getRepo_name().contains(repositorio));
    }
}
