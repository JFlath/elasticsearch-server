package org.elasticsearch.threadpool;

import java.io.IOException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.threadpool.ThreadPoolStatsElement;

public class ServerThreadPoolStatsElement implements ThreadPoolStatsElement {

    private String name;
    private int threads;
    private int queue;
    private int active;
    private long rejected;

    ServerThreadPoolStatsElement() {
    }

    public ServerThreadPoolStatsElement(String name, int threads, int queue, int active, long rejected) {
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
            builder.field(Fields.THREADS, threads);
        }
        if (queue != -1) {
            builder.field(Fields.QUEUE, queue);
        }
        if (active != -1) {
            builder.field(Fields.ACTIVE, active);
        }
        if (rejected != -1) {
            builder.field(Fields.REJECTED, rejected);
        }
        builder.endObject();
        return builder;
    }
}
