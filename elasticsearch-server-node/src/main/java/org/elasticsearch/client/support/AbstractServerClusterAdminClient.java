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

package org.elasticsearch.client.support;

import org.elasticsearch.action.*;
import org.elasticsearch.action.admin.cluster.ServerClusterAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.client.ServerClusterAdminClient;

/**
 *
 */
public abstract class AbstractServerClusterAdminClient extends AbstractClusterAdminClient implements ServerClusterAdminClient {
    
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> 
            RequestBuilder prepareExecute(ServerClusterAction<Request, Response, RequestBuilder> action) {
        return action.newRequestBuilder(this);
    }    
    
    @Override
    public ActionFuture<ClusterRerouteResponse> reroute(final ClusterRerouteRequest request) {
        return execute(ClusterRerouteAction.INSTANCE, request);
    }

    @Override
    public void reroute(final ClusterRerouteRequest request, final ActionListener<ClusterRerouteResponse> listener) {
        execute(ClusterRerouteAction.INSTANCE, request, listener);
    }

    @Override
    public ClusterRerouteRequestBuilder prepareReroute() {
        return new ClusterRerouteRequestBuilder(this);
    }

}
