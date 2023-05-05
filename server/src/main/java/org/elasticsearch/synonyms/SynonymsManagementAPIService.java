/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class SynonymsManagementAPIService {
    private static final FeatureFlag SYNONYMS_API_FEATURE_FLAG = new FeatureFlag("synonyms_api");
    public static final String SYNONYMS_INDEX = ".synonyms";
    public static final String SYNONYMS_ORIGIN = "synonyms";

    public static final String SYNONYMS_FEATURE_NAME = "synonyms";

    private final Client client;

    public static final SystemIndexDescriptor SYNONYMS_DESCRIPTOR = SystemIndexDescriptor.builder()
        .setIndexPattern(SYNONYMS_INDEX + "*")
        .setDescription("Synonyms index for synonyms managed through APIs")
        .setPrimaryIndex(SYNONYMS_INDEX)
        .setMappings(mappings())
        .setSettings(settings())
        .setVersionMetaKey("version")
        .setOrigin(SYNONYMS_ORIGIN)
        .build();

    public SynonymsManagementAPIService(Client client) {
        // TODO Should we set an OriginSettingClient? We would need to check the origin at AuthorizationUtils if we set an
        this.client = client;
    }

    public void putSynonymSet(String resourceName, SynonymSet synonymSet, ActionListener<IndexResponse> listener) {

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            synonymSet.toXContent(builder, ToXContent.EMPTY_PARAMS);

            final IndexRequest indexRequest = new IndexRequest(SYNONYMS_INDEX).opType(DocWriteRequest.OpType.INDEX)
                .id(resourceName)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(builder);
            client.index(indexRequest, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void getSynonymSet(String resourceName, ActionListener<SynonymSet> listener) {
        final GetRequest getRequest = new GetRequest(SYNONYMS_INDEX).id(resourceName).realtime(true);
        client.get(getRequest, new DelegatingIndexNotFoundActionListener<>(resourceName, listener, (l, getResponse) -> {
            if (getResponse.isExists() == false) {
                l.onFailure(new ResourceNotFoundException(resourceName));
                return;
            }
            final BytesReference source = getResponse.getSourceInternal();
            final SynonymSet result = SynonymSet.fromXContentBytes(getResponse.getSourceInternal(), XContentType.JSON);
            l.onResponse(result);
        }));
    }

    public void deleteSynonymSet(String resourceName, ActionListener<DeleteResponse> listener) {
        try {
            final DeleteRequest deleteRequest = new DeleteRequest(SYNONYMS_INDEX).id(resourceName)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client.delete(deleteRequest, new DelegatingIndexNotFoundActionListener<>(resourceName, listener, (l, deleteResponse) -> {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    l.onFailure(new ResourceNotFoundException(resourceName));
                    return;
                }
                l.onResponse(deleteResponse);
            }));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static SynonymSetList mapSearchResponse(SearchResponse response) {
        final List<SynonymSetListItem> listItems = Arrays.stream(response.getHits().getHits())
            .map(SynonymsManagementAPIService::hitToSynonymSetListItem)
            .toList();
        return new SynonymSetList(listItems, response.getHits().getTotalHits().value);
    }

    private static SynonymSetListItem hitToSynonymSetListItem(SearchHit searchHit) {
        @SuppressWarnings("unchecked")
        final List<String> synonyms = (List<String>) searchHit.getSourceAsMap().get(SynonymSet.SYNONYMS_FIELD.getPreferredName());
        return new SynonymSetListItem(searchHit.getId(), (long) synonyms.size());
    }

    public void listSynonymSets(ActionListener<SynonymSetList> listener) {
        try {
            final SearchSourceBuilder source = new SearchSourceBuilder().query(new MatchAllQueryBuilder())
                .docValueField(SynonymSet.SYNONYMS_FIELD.getPreferredName());
            final SearchRequest req = new SearchRequest(SYNONYMS_INDEX).source(source);
            client.search(req, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    listener.onResponse(mapSearchResponse(searchResponse));
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof IndexNotFoundException) {
                        listener.onResponse(new SynonymSetList(Collections.emptyList(), 0L));
                        return;
                    }
                    listener.onFailure(e);
                }
            });
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static XContentBuilder mappings() {
        try {
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject(SINGLE_MAPPING_NAME);
                {
                    builder.startObject("_meta");
                    {
                        builder.field("version", Version.CURRENT.toString());
                    }
                    builder.endObject();
                    builder.field("dynamic", "strict");
                    builder.startObject("properties");
                    {
                        builder.startObject("synonyms");
                        {
                            builder.field("type", "object");
                            builder.field("enabled", "false");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for " + SYNONYMS_INDEX, e);
        }
    }

    static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
            .build();
    }

    public static boolean isEnabled() {
        return SYNONYMS_API_FEATURE_FLAG.isEnabled();
    }

    static class DelegatingIndexNotFoundActionListener<T, R> extends DelegatingActionListener<T, R> {

        private final BiConsumer<ActionListener<R>, T> bc;
        private final String resourceName;

        DelegatingIndexNotFoundActionListener(String resourceName, ActionListener<R> delegate, BiConsumer<ActionListener<R>, T> bc) {
            super(delegate);
            this.bc = bc;
            this.resourceName = resourceName;
        }

        @Override
        public void onResponse(T t) {
            bc.accept(delegate, t);
        }

        @Override
        public void onFailure(Exception e) {
            if (e instanceof IndexNotFoundException) {
                delegate.onFailure(new ResourceNotFoundException(resourceName, e));
                return;
            }
            delegate.onFailure(e);
        }
    }
}
