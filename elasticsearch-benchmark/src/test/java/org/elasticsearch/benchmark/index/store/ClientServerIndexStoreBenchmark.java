package org.elasticsearch.benchmark.index.store;

import org.elasticsearch.client.Client;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import org.elasticsearch.node.Node;
import static org.elasticsearch.node.NodeBuilder.*;

public class ClientServerIndexStoreBenchmark {

    private void createServer() {
        Node server = nodeBuilder()
                .settings(settingsBuilder()
                .put("index.store.type", "memory")
                .put("node.local", true))
                .build();
        Client client = server.client();
    }
}
