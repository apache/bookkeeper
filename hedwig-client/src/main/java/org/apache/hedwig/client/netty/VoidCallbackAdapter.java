package org.apache.hedwig.client.netty;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.util.Callback;

/**
 * Adapts from Callback&lt;T> to Callback&lt;Void>. (Ignores the &lt;T> parameter).
 */
public class VoidCallbackAdapter<T> implements Callback<T> {
    private final Callback<Void> delegate;

    public VoidCallbackAdapter(Callback<Void> delegate){
        this.delegate = delegate;
    }

    @Override
    public void operationFinished(Object ctx, T resultOfOperation) {
        delegate.operationFinished(ctx, null);
    }

    @Override
    public void operationFailed(Object ctx, PubSubException exception) {
        delegate.operationFailed(ctx, exception);
    }
}
