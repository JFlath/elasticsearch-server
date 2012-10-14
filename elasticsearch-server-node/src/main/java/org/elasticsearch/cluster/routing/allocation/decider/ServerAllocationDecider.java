package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;

public abstract class ServerAllocationDecider extends AbstractComponent implements AllocationDecider {
    
    public ServerAllocationDecider() {
        super(EMPTY_SETTINGS);
    }
    
    protected ServerAllocationDecider(Settings settings) {
        super(settings);
    }

    @Override
    public boolean canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return true;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.YES;
    }

    /**
     * Can the provided shard routing remain on the node?
     */
    @Override
    public boolean canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return true;
    }
}
