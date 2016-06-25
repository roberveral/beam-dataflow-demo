package com.github.roberveral.dataflow.common;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

/**
 * A {@link GithubEvent} contains the information of a event of interaction with the Github API.
 * It is serializable with de AvroCoder and has methods to get the information of the fields.
 *
 * @author Roberto Veral
 */
@DefaultCoder(AvroCoder.class)
public class GithubEvent {
    private String id;
    private String type;
    private String actor_id;
    private String actor_login;
    private String repo_id;
    private String repo_name;

    /**
     * Generates a new {@link GithubEvent} with the given information.
     *
     * @param id          event id
     * @param type        event type
     * @param actor_id    user id
     * @param actor_login username
     * @param repo_id     repository id
     * @param repo_name   repository name
     */
    public GithubEvent(String id, String type, String actor_id, String actor_login, String repo_id, String repo_name) {
        this.id = id;
        this.type = type;
        this.actor_id = actor_id;
        this.actor_login = actor_login;
        this.repo_id = repo_id;
        this.repo_name = repo_name;
    }

    /**
     * Obtains the event id from a {@link GithubEvent}
     *
     * @return the event id
     */
    public String getId() {
        return id;
    }

    /**
     * Obtains the event type from a {@link GithubEvent}
     *
     * @return the event type
     */
    public String getType() {
        return type;
    }

    /**
     * Obtains the user id from a {@link GithubEvent}
     *
     * @return the user id
     */
    public String getActor_id() {
        return actor_id;
    }

    /**
     * Obtains the user name from a {@link GithubEvent}
     *
     * @return the user name
     */
    public String getActor_login() {
        return actor_login;
    }

    /**
     * Obtains the repository id from a {@link GithubEvent}
     *
     * @return the repository id
     */
    public String getRepo_id() {
        return repo_id;
    }

    /**
     * Obtains the repository name from a {@link GithubEvent}
     *
     * @return the repository name
     */
    public String getRepo_name() {
        return repo_name;
    }
}