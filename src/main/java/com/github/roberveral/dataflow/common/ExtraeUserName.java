package com.github.roberveral.dataflow.common;

import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;

/**
 * Extracts the username from a {@link GithubEvent}
 *
 * @author Roberto Veral
 */
public class ExtraeUserName extends SimpleFunction<GithubEvent, String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String apply(GithubEvent arg0) {
        return arg0.getActor_login();
    }
}
