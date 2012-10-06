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
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.threadpool.ThreadPoolStatsElement;

/**
 */
public class ServerThreadPoolStats implements ThreadPoolStats {

    public static class Stats implements Streamable, ToXContent {

        private String name;
        private int threads;
        private int queue;
        private int active;
        private long rejected;

        Stats() {

        }

        public Stats(String name, int threads, int queue, int active, long rejected) {
            this.name = name;
            this.threads = threads;
            this.queue = queue;
            this.active = active;
            this.rejected = rejected;
        }

        public String name() {
            return this.name;
        }

        public String getName() {
            return this.name;
        }

        public int threads() {
            return this.threads;
        }

        public int getThreads() {
            return this.threads;
        }

        public int queue() {
            return this.queue;
        }

        public int getQueue() {
            return this.queue;
        }

        public int active() {
            return this.active;
        }

        public int getActive() {
            return this.active;
        }

        public long rejected() {
            return rejected;
        }

        public long getRejected() {
            return rejected;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readUTF();
            threads = in.readInt();
            queue = in.readInt();
            active = in.readInt();
            rejected = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeUTF(name);
            out.writeInt(threads);
            out.writeInt(queue);
            out.writeInt(active);
            out.writeLong(rejected);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name, XContentBuilder.FieldCaseConversion.NONE);
            if (threads != -1) {
                builder.field(ThreadPoolStatsElement.Fields.THREADS, threads);
            }
            if (queue != -1) {
                builder.field(ThreadPoolStatsElement.Fields.QUEUE, queue);
            }
            if (active != -1) {
                builder.field(ThreadPoolStatsElement.Fields.ACTIVE, active);
            }
            if (rejected != -1) {
                builder.field(ThreadPoolStatsElement.Fields.REJECTED, rejected);
            }
            builder.endObject();
            return builder;
        }
    }

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
