package org.elasticsearch.discovery;

import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

public class DiscoveryFactory {
    
    protected static final DiscoveryModule[] discoveries;
    protected static DiscoveryModule defaultDiscovery;

    static {
        List<DiscoveryModule> discos = Lists.newArrayList();
        ServiceLoader<DiscoveryModule> loader = ServiceLoader.load(DiscoveryModule.class);
        Iterator<DiscoveryModule> it = loader.iterator();
        while (it.hasNext()) {
            DiscoveryModule disco = it.next();
            
            discos.add(disco);
            if (defaultDiscovery == null) {
                defaultDiscovery = disco;
            }
        }
        discoveries = discos.toArray(new DiscoveryModule[discos.size()]);
    }
    
    public static DiscoveryModule defaultDiscovery() {
        return defaultDiscovery;
    }
    
    public static DiscoveryModule[] discoveries() {
        return discoveries;
    }
}
