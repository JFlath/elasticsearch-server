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

package org.elasticsearch.benchmark.compress;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.compress.CompressedDirectory;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.File;
import org.testng.annotations.Test;

/**
 */
public class LuceneCompressionBenchmarkTests {

    private final ESLogger logger = Loggers.getLogger(getClass());
    
    @Test
    public void test() throws Exception {
        final long MAX_SIZE = ByteSizeValue.parseBytesSizeValue("50mb").bytes();
        final boolean WITH_TV = true;

        File testFile = new File("target/test/compress/lucene");
        FileSystemUtils.deleteRecursively(testFile);
        testFile.mkdirs();

        FSDirectory uncompressedDir = new NIOFSDirectory(new File(testFile, "uncompressed"));
        IndexWriter uncompressedWriter = new IndexWriter(uncompressedDir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        Compressor lzf = CompressorFactory.compressor("lzf");
        Directory compressedLzfDir = new CompressedDirectory(new NIOFSDirectory(new File(testFile, "compressed_lzf")), lzf, false, "fdt", "tvf");
        IndexWriter compressedLzfWriter = new IndexWriter(compressedLzfDir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        //Directory compressedSnappyDir = new CompressedDirectory(new NIOFSDirectory(new File(testFile, "compressed_snappy")), new XerialSnappyCompressor(), false, "fdt", "tvf");
        //IndexWriter compressedSnappyWriter = new IndexWriter(compressedSnappyDir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        logger.info("feeding data...");
        TestData testData = new TestData();
        while (testData.next() && testData.getTotalSize() < MAX_SIZE) {
            // json
            XContentBuilder builder = XContentFactory.jsonBuilder();
            testData.current(builder);
            builder.close();
            Document doc = new Document();
            doc.add(new Field("_source", builder.bytes().array(), builder.bytes().arrayOffset(), builder.bytes().length()));
            if (WITH_TV) {
                Field field = new Field("text", builder.string(), Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
                doc.add(field);
            }
            uncompressedWriter.addDocument(doc);
            compressedLzfWriter.addDocument(doc);
            //compressedSnappyWriter.addDocument(doc);
        }
        logger.info("optimizing...");
        uncompressedWriter.forceMerge(1);
        compressedLzfWriter.forceMerge(1);
        //compressedSnappyWriter.forceMerge(1);
        uncompressedWriter.waitForMerges();
        compressedLzfWriter.waitForMerges();
        //compressedSnappyWriter.waitForMerges();

        logger.info("done");
        uncompressedWriter.close();
        compressedLzfWriter.close();
        //compressedSnappyWriter.close();

        compressedLzfDir.close();
        //compressedSnappyDir.close();
        uncompressedDir.close();
    }

}
