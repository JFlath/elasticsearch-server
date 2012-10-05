package org.elasticsearch.action.index;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;

public class IndexHelper {

    public static void process(IndexRequest indexRequest, MetaData metaData, String aliasOrIndex, @Nullable MappingMetaData mappingMd, boolean allowIdGeneration) throws ElasticSearchException {
        // resolve the routing if needed
        indexRequest.routing(metaData.resolveIndexRouting(indexRequest.routing(), aliasOrIndex));
        // resolve timestamp if provided externally
        if (indexRequest.timestamp() != null) {
            indexRequest.timestamp(MappingMetaData.Timestamp.parseStringTimestamp(indexRequest.timestamp(),
                    mappingMd != null ? mappingMd.timestamp().dateTimeFormatter() : TimestampFieldMapper.Defaults.DATE_TIME_FORMATTER));
        }
        // extract values if needed
        if (mappingMd != null) {
            MappingMetaData.ParseContext parseContext = mappingMd.createParseContext(indexRequest.id(), indexRequest.routing(), indexRequest.timestamp());

            if (parseContext.shouldParse()) {
                XContentParser parser = null;
                try {
                    parser = XContentHelper.createParser(indexRequest.source());
                    mappingMd.parse(parser, parseContext);
                    if (parseContext.shouldParseId()) {
                        indexRequest.id(parseContext.id());
                    }
                    if (parseContext.shouldParseRouting()) {
                        indexRequest.routing (parseContext.routing());
                    }
                    if (parseContext.shouldParseTimestamp()) {
                        indexRequest.timestamp(parseContext.timestamp());
                        indexRequest.timestamp(MappingMetaData.Timestamp.parseStringTimestamp(indexRequest.timestamp(), mappingMd.timestamp().dateTimeFormatter()));
                    }
                } catch (Exception e) {
                    throw new ElasticSearchParseException("failed to parse doc to extract routing/timestamp", e);
                } finally {
                    if (parser != null) {
                        parser.close();
                    }
                }
            }

            // might as well check for routing here
            if (mappingMd.routing().required() && indexRequest.routing() == null) {
                throw new RoutingMissingException(indexRequest.index(), indexRequest.type(), indexRequest.id());
            }
        }

        // generate id if not already provided and id generation is allowed
        if (allowIdGeneration) {
            if (indexRequest.id() == null) {
                indexRequest.id(UUID.randomBase64UUID());
                // since we generate the id, change it to CREATE
                indexRequest.opType(IndexRequest.OpType.CREATE);
            }
        }

        // generate timestamp if not provided, we always have one post this stage...
        if (indexRequest.timestamp() == null) {
            indexRequest.timestamp (Long.toString(System.currentTimeMillis()));
        }
    }
    
}
