
package org.elasticsearch.client.node;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.GenericClient;
import org.elasticsearch.client.IngestClient;
import org.elasticsearch.client.support.AbstractServerIngestClient;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

public class NodeIngestClient extends AbstractServerIngestClient {
    
    private final Settings settings;

    private final ThreadPool threadPool;

    private final ImmutableMap<Action, TransportAction> actions;

    @Inject
    public NodeIngestClient(Settings settings, ThreadPool threadPool,
             Map<GenericAction, TransportAction> actions) {
        this.settings = settings;
        this.threadPool = threadPool;
        MapBuilder<Action, TransportAction> actionsBuilder = new MapBuilder<Action, TransportAction>();
        for (Map.Entry<GenericAction, TransportAction> entry : actions.entrySet()) {
            if (entry.getKey() instanceof Action) {
                actionsBuilder.put((Action) entry.getKey(), entry.getValue());
            }
        }
        this.actions = actionsBuilder.immutableMap();
    }

    @Override
    public Settings settings() {
        return this.settings;
    }

    @Override
    public ThreadPool threadPool() {
        return threadPool;
    }
    
    @Override
    public void close() {
        // nothing really to do
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>, IngestClient extends GenericClient> 
            ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, IngestClient> action, Request request) {
        TransportAction<Request, Response> transportAction = actions.get(action);
        return transportAction.execute(request);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>, IngestClient extends GenericClient> 
            void execute(Action<Request, Response, RequestBuilder, IngestClient> action, Request request, ActionListener<Response> listener) {
        TransportAction<Request, Response> transportAction = actions.get(action);
        transportAction.execute(request, listener);
    }
}
