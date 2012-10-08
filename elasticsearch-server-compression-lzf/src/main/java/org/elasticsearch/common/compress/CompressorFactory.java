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

package org.elasticsearch.common.compress;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import org.elasticsearch.common.compress.lzf.LZFCompressor;

/**
 */
public class CompressorFactory extends BasicCompressorFactory {

    public static synchronized void setDefaultCompressor(Compressor defaultCompressor) {
         CompressorFactory.defaultCompressor = defaultCompressor;
    }

    public static Compressor defaultCompressor() {
        if (defaultCompressor == null) {
            // fallback, in case ServiceLoader did not find a compressor
            setDefaultCompressor(new LZFCompressor());
        }
        return (Compressor)defaultCompressor;
    }

    public static boolean isCompressed(BytesReference bytes) {
        return compressor(bytes) != null;
    }

    public static boolean isCompressed(byte[] data) {
        return compressor(data, 0, data.length) != null;
    }

    public static boolean isCompressed(byte[] data, int offset, int length) {
        return compressor(data, offset, length) != null;
    }

    public static boolean isCompressed(IndexInput in) throws IOException {
        return compressor(in) != null;
    }

    @Nullable
    public static Compressor compressor(BytesReference bytes) {
        for (BasicCompressor compressor : compressors) {
            if (compressor.isCompressed(bytes) && compressor instanceof Compressor) {
                return (Compressor)compressor;
            }
        }
        return null;
    }

    @Nullable
    public static Compressor compressor(byte[] data) {
        return compressor(data, 0, data.length);
    }

    @Nullable
    public static Compressor compressor(byte[] data, int offset, int length) {
        for (BasicCompressor compressor : compressors) {
            if (compressor.isCompressed(data, offset, length) && compressor instanceof Compressor) {
                return (Compressor)compressor;
            }
        }
        return null;
    }

    @Nullable
    public static Compressor compressor(ChannelBuffer buffer) {
        for (BasicCompressor compressor : compressors) {
            if (compressor instanceof Compressor) {
                Compressor c = (Compressor)compressor;
                if (c.isCompressed(buffer)) {
                    return c;
                }
            }
        }
        return null;
    }

    @Nullable
    public static Compressor compressor(IndexInput in) throws IOException {
        for (BasicCompressor compressor : compressors) {
            if (compressor instanceof Compressor) {
                Compressor c = (Compressor)compressor;
                if (c.isCompressed(in)) {
                    return c;
                }
            }
        }
        return null;
    }

    public static Compressor compressor(String type) {
        return (Compressor)compressorsByType.get(type);
    }

    /**
     * Uncompress the provided data, data can be detected as compressed using {@link #isCompressed(byte[], int, int)}.
     */
    public static BytesReference uncompressIfNeeded(BytesReference bytes) throws IOException {
        Compressor compressor = compressor(bytes);
        if (compressor != null) {
            if (bytes.hasArray()) {
                return new BytesArray(compressor.uncompress(bytes.array(), bytes.arrayOffset(), bytes.length()));
            }
            StreamInput compressed = compressor.streamInput(bytes.streamInput());
            CachedStreamOutput.Entry entry = CachedStreamOutput.popEntry();
            try {
                Streams.copy(compressed, entry.bytes());
                compressed.close();
                return new BytesArray(entry.bytes().bytes().toBytes());
            } finally {
                CachedStreamOutput.pushEntry(entry);
            }
        }
        return bytes;
    }
}
