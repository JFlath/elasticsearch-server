/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.transport.support;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.*;
import org.elasticsearch.client.GenericClient;
import org.elasticsearch.client.internal.InternalGenericClient;
import org.elasticsearch.client.support.AbstractSearchClient;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

/**
 *
 */
public class InternalTransportSearchClient extends AbstractSearchClient implements InternalGenericClient {

    private final Settings settings;

    private final TransportClientNodesService nodesService;
    
    private final ThreadPool threadPool;

    private final ImmutableMap<Action, TransportActionNodeProxy> actions;

    @Inject
    public InternalTransportSearchClient(Settings settings, TransportService transportService,
                                   TransportClientNodesService nodesService, ThreadPool threadPool,
                                   Map<String, GenericAction> actions) {
        this.settings = settings;
        this.nodesService = nodesService;
        this.threadPool = threadPool;

        MapBuilder<Action, TransportActionNodeProxy> actionsBuilder = new MapBuilder<Action, TransportActionNodeProxy>();
        for (GenericAction action : actions.values()) {
            if (action instanceof Action) {
                actionsBuilder.put((Action) action, new TransportActionNodeProxy(settings, action, transportService));
            }
        }
        this.actions = actionsBuilder.immutableMap();
    }

    @Override
    public void close() {
        // nothing to do here
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
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>, IngestClient extends GenericClient> 
            ActionFuture<Response> execute(final Action<Request, Response, RequestBuilder, IngestClient> action, final Request request) {
        final TransportActionNodeProxy<Request, Response> proxy = actions.get(action);
        return nodesService.execute(new TransportClientNodesService.NodeCallback<ActionFuture<Response>>() {
            @Override
            public ActionFuture<Response> doWithNode(DiscoveryNode node) throws ElasticSearchException {
                return proxy.execute(node, request);
            }
        });
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>, IngestClient extends GenericClient> 
            void execute(final Action<Request, Response, RequestBuilder, IngestClient> action, final Request request, ActionListener<Response> listener) {
        final TransportActionNodeProxy<Request, Response> proxy = actions.get(action);
        nodesService.execute(new TransportClientNodesService.NodeListenerCallback<Response>() {
            @Override
            public void doWithNode(DiscoveryNode node, ActionListener<Response> listener) throws ElasticSearchException {
                proxy.execute(node, request, listener);
            }
        }, listener);
    }
}
