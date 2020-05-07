package org.apache.bookkeeper.auth;

/**
 * Callbacks for AuthProviders.
 */
public abstract class AuthCallbacks {

    /**
     * Generic callback used in authentication providers.
     */
    public interface GenericCallback<T> {

        void operationComplete(int rc, T result);
    }
}
