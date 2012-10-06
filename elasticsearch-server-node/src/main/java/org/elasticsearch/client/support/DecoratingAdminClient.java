package org.elasticsearch.client.support;

import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;

public class DecoratingAdminClient implements AdminClient {

    private final ClusterAdminClient clusterAdminClient;
    private final IndicesAdminClient indicesAdminClient;
    
    public DecoratingAdminClient(ClusterAdminClient clusterAdminClient,
            IndicesAdminClient indicesAdminClient) {
        this.clusterAdminClient = clusterAdminClient;
        this.indicesAdminClient = indicesAdminClient;
    }
    
    @Override
    public ClusterAdminClient cluster() {
        return clusterAdminClient;
    }

    @Override
    public IndicesAdminClient indices() {
        return indicesAdminClient;
    }
    
}
