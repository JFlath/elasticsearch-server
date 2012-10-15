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

package org.elasticsearch.client.transport;

import com.google.common.collect.ImmutableList;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.mlt.MoreLikeThisRequest;
import org.elasticsearch.action.percolate.PercolateRequest;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.support.DecoratingAdminClient;
import org.elasticsearch.client.IngestClient;
import org.elasticsearch.client.transport.support.InternalTransportClusterAdminClient;
import org.elasticsearch.client.transport.support.InternalTransportIndicesAdminClient;
import org.elasticsearch.client.transport.support.InternalTransportIngestClient;
import org.elasticsearch.client.transport.support.InternalTransportSearchClient;
import org.elasticsearch.cluster.ClusterNameModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.io.CachedStreams;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadLocals;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.node.internal.InternalSettingsPerparer;
import org.elasticsearch.plugins.PluginsModule;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.TransportSearchModule;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.transport.TransportModule;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.TimeUnit;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.explain.ExplainRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.mlt.MoreLikeThisRequestBuilder;
import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.threadpool.server.ServerThreadPool;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * The transport client allows to create a client that is not part of the cluster, but simply connects to one
 * or more nodes directly by adding their respective addresses using {@link #addTransportAddress(org.elasticsearch.common.transport.TransportAddress)}.
 * <p/>
 * <p>The transport client important modules used is the {@link org.elasticsearch.transport.TransportModule} which is
 * started in client mode (only connects, no bind).
 */
public class TransportClient extends AbstractComponent implements Client {

    private final Injector injector;

    private final Environment environment;

    private final Settings settings;

    private final PluginsService pluginsService;

    private final TransportClientNodesService nodesService;
    
    private final AdminClient adminClient;

    private final InternalTransportIngestClient internalIngestClient;
    
    private final InternalTransportSearchClient internalSearchClient;
    
    private final InternalTransportClusterAdminClient internalClusterAdminClient;
    
    private final InternalTransportIndicesAdminClient internalIndicesAdminClient;
    
    /**
     * Constructs a new transport client with settings loaded either from the classpath or the file system (the
     * <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with <tt>config/</tt>).
     */
    public TransportClient() throws ElasticSearchException {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS, true);
    }

    /**
     * Constructs a new transport client with explicit settings and settings loaded either from the classpath or the file
     * system (the <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with <tt>config/</tt>).
     */
    public TransportClient(Settings settings) {
        this(settings, true);
    }

    /**
     * Constructs a new transport client with explicit settings and settings loaded either from the classpath or the file
     * system (the <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with <tt>config/</tt>).
     */
    public TransportClient(Settings.Builder settings) {
        this(settings.build(), true);
    }

    /**
     * Constructs a new transport client with the provided settings and the ability to control if settings will
     * be loaded from the classpath / file system (the <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with
     * <tt>config/</tt>).
     *
     * @param settings           The explicit settings.
     * @param loadConfigSettings <tt>true</tt> if settings should be loaded from the classpath/file system.
     * @throws ElasticSearchException
     */
    public TransportClient(Settings.Builder settings, boolean loadConfigSettings) throws ElasticSearchException {
        this(settings.build(), loadConfigSettings);
    }

    /**
     * Constructs a new transport client with the provided settings and the ability to control if settings will
     * be loaded from the classpath / file system (the <tt>elasticsearch.(yml|json)</tt> files optionally prefixed with
     * <tt>config/</tt>).
     *
     * @param pSettings          The explicit settings.
     * @param loadConfigSettings <tt>true</tt> if settings should be loaded from the classpath/file system.
     * @throws ElasticSearchException
     */
    public TransportClient(Settings pSettings, boolean loadConfigSettings) throws ElasticSearchException {
        super(pSettings);
        Tuple<Settings, Environment> tuple = InternalSettingsPerparer.prepareSettings(pSettings, loadConfigSettings);
        Settings settings = settingsBuilder().put(tuple.v1())
                .put("network.server", false)
                .put("node.client", true)
                .build();
        this.environment = tuple.v2();

        this.pluginsService = new PluginsService(settings, tuple.v2());
        this.settings = pluginsService.updatedSettings();

        CompressorFactory.configure(this.settings);

        ModulesBuilder modules = new ModulesBuilder();
        modules.add(new PluginsModule(this.settings, pluginsService));
        modules.add(new EnvironmentModule(environment));
        modules.add(new SettingsModule(this.settings));
        modules.add(new NetworkModule());
        modules.add(new ClusterNameModule(this.settings));
        modules.add(new ThreadPoolModule(this.settings));
        modules.add(new TransportSearchModule());
        modules.add(new TransportModule(this.settings));
        modules.add(new ActionModule(true));
        modules.add(new ClientTransportModule());

        injector = modules.createInjector();

        injector.getInstance(TransportService.class).start();

        nodesService = injector.getInstance(TransportClientNodesService.class);

        internalIngestClient = injector.getInstance(InternalTransportIngestClient.class);
        internalSearchClient = injector.getInstance(InternalTransportSearchClient.class);
        internalClusterAdminClient = injector.getInstance(InternalTransportClusterAdminClient.class);
        internalIndicesAdminClient = injector.getInstance(InternalTransportIndicesAdminClient.class);
        adminClient = new DecoratingAdminClient(internalClusterAdminClient, internalIndicesAdminClient);
    }

    public AdminClient admin() {
        return adminClient;
    }
    
    /**
     * Returns the current registered transport addresses to use (added using
     * {@link #addTransportAddress(org.elasticsearch.common.transport.TransportAddress)}.
     */
    public ImmutableList<TransportAddress> transportAddresses() {
        return nodesService.transportAddresses();
    }

    /**
     * Returns the current connected transport nodes that this client will use.
     * <p/>
     * <p>The nodes include all the nodes that are currently alive based on the transport
     * addresses provided.
     */
    public ImmutableList<DiscoveryNode> connectedNodes() {
        return nodesService.connectedNodes();
    }

    /**
     * Returns the listed nodes in the transport client (ones added to it).
     */
    public ImmutableList<DiscoveryNode> listedNodes() {
        return nodesService.listedNodes();
    }

    /**
     * Adds a transport address that will be used to connect to.
     * <p/>
     * <p>The Node this transport address represents will be used if its possible to connect to it.
     * If it is unavailable, it will be automatically connected to once it is up.
     * <p/>
     * <p>In order to get the list of all the current connected nodes, please see {@link #connectedNodes()}.
     */
    public TransportClient addTransportAddress(TransportAddress transportAddress) {
        nodesService.addTransportAddresses(transportAddress);
        return this;
    }

    /**
     * Adds a list of transport addresses that will be used to connect to.
     * <p/>
     * <p>The Node this transport address represents will be used if its possible to connect to it.
     * If it is unavailable, it will be automatically connected to once it is up.
     * <p/>
     * <p>In order to get the list of all the current connected nodes, please see {@link #connectedNodes()}.
     */
    public TransportClient addTransportAddresses(TransportAddress... transportAddress) {
        nodesService.addTransportAddresses(transportAddress);
        return this;
    }

    /**
     * Removes a transport address from the list of transport addresses that are used to connect to.
     */
    public TransportClient removeTransportAddress(TransportAddress transportAddress) {
        nodesService.removeTransportAddress(transportAddress);
        return this;
    }

    /**
     * Closes the client.
     */
    public void close() {
        
        internalIndicesAdminClient.close();
        internalClusterAdminClient.close();
        internalSearchClient.close();
        internalIngestClient.close();
        
        injector.getInstance(TransportClientNodesService.class).close();
        injector.getInstance(TransportService.class).close();
        try {
            injector.getInstance(MonitorService.class).close();
        } catch (Exception e) {
            // ignore, might not be bounded
        }

        for (Class<? extends LifecycleComponent> plugin : pluginsService.services()) {
            injector.getInstance(plugin).close();
        }

        injector.getInstance(ServerThreadPool.class).shutdown();
        try {
            injector.getInstance(ServerThreadPool.class).awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            injector.getInstance(ServerThreadPool.class).shutdownNow();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }

        CacheRecycler.clear();
        CachedStreams.clear();
        ThreadLocals.clearReferencesThreadLocals();
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> 
            ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, IngestClient> action, Request request) {
        return internalIngestClient.execute(action, request);
    }

    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> 
            void execute(Action<Request, Response, RequestBuilder, IngestClient> action, Request request, ActionListener<Response> listener) {
        internalIngestClient.execute(action, request, listener);
    }

    public ActionFuture<IndexResponse> index(IndexRequest request) {
        return internalIngestClient.index(request);
    }

    public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
        internalIngestClient.index(request, listener);
    }

    public ActionFuture<UpdateResponse> update(UpdateRequest request) {
        return internalIngestClient.update(request);
    }

    public void update(UpdateRequest request, ActionListener<UpdateResponse> listener) {
        internalIngestClient.update(request, listener);
    }

    public ActionFuture<DeleteResponse> delete(DeleteRequest request) {
        return internalIngestClient.delete(request);
    }

    public void delete(DeleteRequest request, ActionListener<DeleteResponse> listener) {
        internalIngestClient.delete(request, listener);
    }

    public ActionFuture<BulkResponse> bulk(BulkRequest request) {
        return internalIngestClient.bulk(request);
    }

    public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
        internalIngestClient.bulk(request, listener);
    }

    public ActionFuture<DeleteByQueryResponse> deleteByQuery(DeleteByQueryRequest request) {
        return internalSearchClient.deleteByQuery(request);
    }

    public void deleteByQuery(DeleteByQueryRequest request, ActionListener<DeleteByQueryResponse> listener) {
        internalSearchClient.deleteByQuery(request, listener);
    }

    public ActionFuture<GetResponse> get(GetRequest request) {
        return internalIngestClient.get(request);
    }

    public void get(GetRequest request, ActionListener<GetResponse> listener) {
        internalIngestClient.get(request, listener);
    }

    public ActionFuture<MultiGetResponse> multiGet(MultiGetRequest request) {
        return internalIngestClient.multiGet(request);
    }

    public void multiGet(MultiGetRequest request, ActionListener<MultiGetResponse> listener) {
        internalIngestClient.multiGet(request, listener);
    }

    public ActionFuture<CountResponse> count(CountRequest request) {
        return internalSearchClient.count(request);
    }

    public void count(CountRequest request, ActionListener<CountResponse> listener) {
        internalSearchClient.count(request, listener);
    }

    public ActionFuture<SearchResponse> search(SearchRequest request) {
        return internalSearchClient.search(request);
    }

    public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        internalSearchClient.search(request, listener);
    }

    public ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) {
        return internalSearchClient.searchScroll(request);
    }

    public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
        internalSearchClient.searchScroll(request, listener);
    }

    public ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request) {
        return internalSearchClient.multiSearch(request);
    }

    public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        internalSearchClient.multiSearch(request, listener);
    }

    public ActionFuture<SearchResponse> moreLikeThis(MoreLikeThisRequest request) {
        return internalSearchClient.moreLikeThis(request);
    }

    public void moreLikeThis(MoreLikeThisRequest request, ActionListener<SearchResponse> listener) {
        internalSearchClient.moreLikeThis(request, listener);
    }

    public ActionFuture<PercolateResponse> percolate(PercolateRequest request) {
        return internalIngestClient.percolate(request);
    }

    public void percolate(PercolateRequest request, ActionListener<PercolateResponse> listener) {
        internalIngestClient.percolate(request, listener);
    }

    public ActionFuture<ExplainResponse> explain(ExplainRequest request) {
        return internalSearchClient.explain(request);
    }

    public void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        internalSearchClient.explain(request, listener);
    }

    @Override
    public IndexRequestBuilder prepareIndex() {
        return internalIngestClient.prepareIndex();
    }

    @Override
    public UpdateRequestBuilder prepareUpdate() {
        return internalIngestClient.prepareUpdate();
    }

    @Override
    public UpdateRequestBuilder prepareUpdate(String index, String type, String id) {
        return internalIngestClient.prepareUpdate(index, type, id);
    }

    @Override
    public IndexRequestBuilder prepareIndex(String index, String type) {
        return internalIngestClient.prepareIndex(index, type);
    }

    @Override
    public IndexRequestBuilder prepareIndex(String index, String type, String id) {
        return internalIngestClient.prepareIndex(index, type, id);
    }

    @Override
    public DeleteRequestBuilder prepareDelete() {
        return internalIngestClient.prepareDelete();
    }

    @Override
    public DeleteRequestBuilder prepareDelete(String index, String type, String id) {
        return internalIngestClient.prepareDelete(index, type, id);
    }

    @Override
    public BulkRequestBuilder prepareBulk() {
        return internalIngestClient.prepareBulk();
    }

    @Override
    public DeleteByQueryRequestBuilder prepareDeleteByQuery(String... indices) {
        return internalSearchClient.prepareDeleteByQuery(indices);
    }

    @Override
    public GetRequestBuilder prepareGet() {
        return internalIngestClient.prepareGet();
    }

    @Override
    public GetRequestBuilder prepareGet(String index, String type, String id) {
        return internalIngestClient.prepareGet(index, type, id);
    }

    @Override
    public MultiGetRequestBuilder prepareMultiGet() {
        return internalIngestClient.prepareMultiGet();
    }

    @Override
    public CountRequestBuilder prepareCount(String... indices) {
        return internalSearchClient.prepareCount(indices);
    }

    @Override
    public SearchRequestBuilder prepareSearch(String... indices) {
        return internalSearchClient.prepareSearch(indices);
    }

    @Override
    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return internalSearchClient.prepareSearchScroll(scrollId);
    }

    @Override
    public MultiSearchRequestBuilder prepareMultiSearch() {
        return internalSearchClient.prepareMultiSearch();
    }

    @Override
    public MoreLikeThisRequestBuilder prepareMoreLikeThis(String index, String type, String id) {
        return internalSearchClient.prepareMoreLikeThis(index, type, id);
    }

    @Override
    public PercolateRequestBuilder preparePercolate(String index, String type) {
        return internalIngestClient.preparePercolate(index, type);
    }

    @Override
    public ExplainRequestBuilder prepareExplain(String index, String type, String id) {
        return internalSearchClient.prepareExplain(index, type, id);
    }
}
