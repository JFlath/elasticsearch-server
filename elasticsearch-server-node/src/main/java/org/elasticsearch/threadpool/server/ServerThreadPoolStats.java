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

package org.elasticsearch.threadpool.server;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.threadpool.ThreadPoolStatsElement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 */
public class ServerThreadPoolStats implements ThreadPoolStats {

 
    private List<ThreadPoolStatsElement> stats;

    ServerThreadPoolStats() {

    }

    public ServerThreadPoolStats(List<ThreadPoolStatsElement> stats) {
        this.stats = stats;
    }

    @Override
    public Iterator<ThreadPoolStatsElement> iterator() {
        return stats.iterator();
    }

    public static ServerThreadPoolStats readThreadPoolStats(StreamInput in) throws IOException {
        ServerThreadPoolStats stats = new ServerThreadPoolStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        stats = new ArrayList(size);
        for (int i = 0; i < size; i++) {
            ThreadPoolStatsElement element = new ServerThreadPoolStatsElement();
            element.readFrom(in);
            stats.add(element);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(stats.size());
        for (ThreadPoolStatsElement stat : stats) {
            stat.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(ThreadPoolStatsElement.Fields.THREAD_POOL);
        for (ThreadPoolStatsElement stat : stats) {
            stat.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
