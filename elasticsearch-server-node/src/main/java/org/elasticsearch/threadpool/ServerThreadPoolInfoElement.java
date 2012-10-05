package org.elasticsearch.threadpool;

import java.io.IOException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.threadpool.ThreadPoolInfoElement;

public class ServerThreadPoolInfoElement implements ThreadPoolInfoElement {

    private String name;
    private String type;
    private int min;
    private int max;
    private TimeValue keepAlive;
    private SizeValue capacity;

    ServerThreadPoolInfoElement() {
    }

    public ServerThreadPoolInfoElement(String name, String type) {
        this(name, type, -1);
    }

    public ServerThreadPoolInfoElement(String name, String type, int size) {
        this(name, type, size, size, null, null);
    }

    public ServerThreadPoolInfoElement(String name, String type, int min, int max, @Nullable TimeValue keepAlive, @Nullable SizeValue capacity) {
        this.name = name;
        this.type = type;
        this.min = min;
        this.max = max;
        this.keepAlive = keepAlive;
        this.capacity = capacity;
    }

    public String name() {
        return this.name;
    }

    public String getName() {
        return this.name;
    }

    public String type() {
        return this.type;
    }

    public String getType() {
        return this.type;
    }

    public int min() {
        return this.min;
    }

    public int getMin() {
        return this.min;
    }

    public int max() {
        return this.max;
    }

    public int getMax() {
        return this.max;
    }

    @Nullable
    public TimeValue keepAlive() {
        return this.keepAlive;
    }

    @Nullable
    public TimeValue getKeepAlive() {
        return this.keepAlive;
    }

    @Nullable
    public SizeValue capacity() {
        return this.capacity;
    }

    @Nullable
    public SizeValue getCapacity() {
        return this.capacity;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        type = in.readUTF();
        min = in.readInt();
        max = in.readInt();
        if (in.readBoolean()) {
            keepAlive = TimeValue.readTimeValue(in);
        }
        if (in.readBoolean()) {
            capacity = SizeValue.readSizeValue(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(type);
        out.writeInt(min);
        out.writeInt(max);
        if (keepAlive == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            keepAlive.writeTo(out);
        }
        if (capacity == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            capacity.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name, XContentBuilder.FieldCaseConversion.NONE);
        builder.field(ThreadPoolInfoElement.Fields.TYPE, type);
        if (min != -1) {
            builder.field(ThreadPoolInfoElement.Fields.MIN, min);
        }
        if (max != -1) {
            builder.field(ThreadPoolInfoElement.Fields.MAX, max);
        }
        if (keepAlive != null) {
            builder.field(ThreadPoolInfoElement.Fields.KEEP_ALIVE, keepAlive.toString());
        }
        if (capacity != null) {
            builder.field(ThreadPoolInfoElement.Fields.CAPACITY, capacity.toString());
        }
        builder.endObject();
        return builder;
    }

}
