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

package org.elasticsearch.client.node;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.internal.InternalGenericClient;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.GenericAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.mlt.MoreLikeThisRequestBuilder;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.GenericClient;
import org.elasticsearch.client.support.DecoratingAdminClient;

import java.util.Map;

/**
 * The Node Client is useful for plugins that execute actions inside the cluster.
 */
public class NodeClient implements InternalGenericClient, Client {

    private final Settings settings;

    private final ThreadPool threadPool;

    private final NodeIngestClient ingestClient;
    
    private final NodeSearchClient searchClient;

    private final AdminClient adminClient;

    private final ImmutableMap<Action, TransportAction> actions;

    @Inject
    public NodeClient(Settings settings, ThreadPool threadPool, 
            NodeIngestClient ingestClient, 
            NodeSearchClient searchClient, 
            NodeClusterAdminClient clusterAdminClient, 
            NodeIndicesAdminClient indicesAdminClient,
            Map<GenericAction, TransportAction> actions) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.ingestClient = ingestClient;
        this.searchClient = searchClient;
        this.adminClient = new DecoratingAdminClient(clusterAdminClient, indicesAdminClient);
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
        return this.threadPool;
    }

    @Override
    public void close() {
        // nothing really to do
    }

    @Override
    public AdminClient admin() {
        return this.adminClient;
    }
    
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>, MyNodeClient extends GenericClient> 
            ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, MyNodeClient> action, Request request) {
        TransportAction<Request, Response> transportAction = actions.get(action);
        return transportAction.execute(request);
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>, MyNodeClient extends GenericClient> 
            void execute(Action<Request, Response, RequestBuilder, MyNodeClient> action, Request request, ActionListener<Response> listener) {
        TransportAction<Request, Response> transportAction = actions.get(action);
        transportAction.execute(request, listener);
    }

    @Override
    public ActionFuture<IndexResponse> index(IndexRequest request) {
        return ingestClient.index(request);
    }

    @Override
    public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        ingestClient.index(request, listener);
    }

    @Override
    public IndexRequestBuilder prepareIndex() {
        return ingestClient.prepareIndex();
    }

    @Override
    public ActionFuture<UpdateResponse> update(UpdateRequest request) {
        return ingestClient.update(request);
    }

    @Override
    public void update(UpdateRequest request, ActionListener<UpdateResponse> listener) {
        ingestClient.update(request, listener);
    }

    @Override
    public UpdateRequestBuilder prepareUpdate() {
        return ingestClient.prepareUpdate();
    }

    @Override
    public UpdateRequestBuilder prepareUpdate(String index, String type, String id) {
        return ingestClient.prepareUpdate(index, type, id);
    }

    @Override
    public IndexRequestBuilder prepareIndex(String index, String type) {
        return ingestClient.prepareIndex(index, type);
    }

    @Override
    public IndexRequestBuilder prepareIndex(String index, String type, String id) {
        return ingestClient.prepareIndex(index, type, id);
    }

    @Override
    public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return ingestClient.delete(request);
    }

    @Override
    public void delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        ingestClient.delete(request, listener);
    }

    @Override
    public DeleteRequestBuilder prepareDelete() {
        return ingestClient.prepareDelete();
    }

    @Override
    public DeleteRequestBuilder prepareDelete(String index, String type, String id) {
        return ingestClient.prepareDelete(index, type, id);
    }

    @Override
    public ActionFuture<BulkResponse> bulk(BulkRequest request) {
        return ingestClient.bulk(request);
    }

    @Override
    public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        ingestClient.bulk(request, listener);
    }

    @Override
    public BulkRequestBuilder prepareBulk() {
        return ingestClient.prepareBulk();
    }

    @Override
    public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request) {
        return searchClient.deleteByQuery(request);
    }

    @Override
    public void deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        searchClient.deleteByQuery(request, listener);
    }

    @Override
    public DeleteByQueryRequestBuilder prepareDeleteByQuery(String... indices) {
        return searchClient.prepareDeleteByQuery(indices);
    }

    @Override
    public ActionFuture<GetResponse> get(GetRequest request) {
        return ingestClient.get(request);
    }

    @Override
    public void get(GetRequest request, ActionListener<GetResponse> listener) {
        ingestClient.get(request, listener);
    }

    @Override
    public GetRequestBuilder prepareGet() {
        return ingestClient.prepareGet();
    }

    @Override
    public GetRequestBuilder prepareGet(String index, String type, String id) {
        return ingestClient.prepareGet(index, type, id);
    }

    @Override
    public ActionFuture<MultiGetResponse> multiGet(MultiGetRequest request) {
        return ingestClient.multiGet(request);
    }

    @Override
    public void multiGet(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        ingestClient.multiGet(request, listener);
    }

    @Override
    public MultiGetRequestBuilder prepareMultiGet() {
        return ingestClient.prepareMultiGet();
    }

    @Override
    public ActionFuture<CountResponse> count(CountRequest request) {
        return searchClient.count(request);
    }

    @Override
    public void count(CountRequest request, ActionListener<CountResponse> listener) {
        searchClient.count(request, listener);
    }

    @Override
    public CountRequestBuilder prepareCount(String... indices) {
        return searchClient.prepareCount(indices);
    }

    @Override
    public ActionFuture<SearchResponse> search(SearchRequest request) {
        return searchClient.search(request);
    }

    @Override
    public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        searchClient.search(request, listener);
    }

    @Override
    public SearchRequestBuilder prepareSearch(String... indices) {
        return searchClient.prepareSearch(indices);
    }

    @Override
    public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        return searchClient.searchScroll(request);
    }

    @Override
    public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        searchClient.searchScroll(request, listener);
    }

    @Override
    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return searchClient.prepareSearchScroll(scrollId);
    }

    @Override
    public ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request) {
        return searchClient.multiSearch(request);
    }

    @Override
    public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        searchClient.multiSearch(request, listener);
    }

    @Override
    public MultiSearchRequestBuilder prepareMultiSearch() {
        return searchClient.prepareMultiSearch();
    }

    @Override
    public ActionFuture<SearchResponse> moreLikeThis(MoreLikeThisRequest request) {
        return searchClient.moreLikeThis(request);
    }

    @Override
    public void moreLikeThis(MoreLikeThisRequest request, ActionListener<SearchResponse> listener) {
        searchClient.moreLikeThis(request, listener);
    }

    @Override
    public MoreLikeThisRequestBuilder prepareMoreLikeThis(String index, String type, String id) {
        return searchClient.prepareMoreLikeThis(index, type, id);
    }

    @Override
    public ActionFuture<PercolateResponse> percolate(PercolateRequest request) {
        return ingestClient.percolate(request);
    }

    @Override
    public void percolate(PercolateRequest request, ActionListener<PercolateResponse> listener) {
        ingestClient.percolate(request, listener);
    }

    @Override
    public PercolateRequestBuilder preparePercolate(String index, String type) {
        return ingestClient.preparePercolate(index, type);
    }

    @Override
    public ExplainRequestBuilder prepareExplain(String index, String type, String id) {
        return searchClient.prepareExplain(index, type, id);
    }

    @Override
    public ActionFuture<ExplainResponse> explain(ExplainRequest request) {
        return searchClient.explain(request);
    }

    @Override
    public void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        searchClient.explain(request, listener);
    }

}
