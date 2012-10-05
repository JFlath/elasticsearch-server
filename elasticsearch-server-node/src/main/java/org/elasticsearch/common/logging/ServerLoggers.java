package org.elasticsearch.common.logging;

import com.google.common.collect.Lists;
import java.util.List;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.river.RiverName;

public class ServerLoggers extends Loggers {
    
    public static ESLogger getLogger(Class clazz, Settings settings, ShardId shardId, String... prefixes) {
        return getLogger(clazz, settings, shardId.index(), Lists.asList(Integer.toString(shardId.id()), prefixes).toArray(new String[0]));
    }

    public static ESLogger getLogger(Class clazz, Settings settings, Index index, String... prefixes) {
        return getLogger(clazz, settings, Lists.asList(SPACE, index.name(), prefixes).toArray(new String[0]));
    }

    public static ESLogger getLogger(Class clazz, Settings settings, RiverName riverName, String... prefixes) {
        List<String> l = Lists.newArrayList();
        l.add(SPACE);
        l.add(riverName.type());
        l.add(riverName.name());
        l.addAll(Lists.newArrayList(prefixes));
        return getLogger(clazz, settings, l.toArray(new String[l.size()]));
    }
}
