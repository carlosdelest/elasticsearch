/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.synonyms.SynonymSet;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetSynonymsAction extends ActionType<GetSynonymsAction.Response> {

    public static final GetSynonymsAction INSTANCE = new GetSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/get";

    public GetSynonymsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {
        private final String resourceName;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.resourceName = in.readString();
        }

        public Request(String resourceName) throws IOException {
            if (Strings.isEmpty(resourceName)) {
                throw new IllegalArgumentException("synonym set must be specified");
            }

            this.resourceName = resourceName;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(resourceName);
        }

        public String resourceName() {
            return resourceName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return resourceName.equals(request.resourceName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resourceName);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final SynonymSet synonymSet;

        public Response(StreamInput in) throws IOException {
            super(in);
            synonymSet = new SynonymSet(in);
        }

        public Response(SynonymSet synonymSet) {
            Objects.requireNonNull(synonymSet, "Synonym set must be specified");
            this.synonymSet = synonymSet;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.synonymSet.writeTo(out);
        }

        public SynonymSet synonymSet() {
            return synonymSet;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            synonymSet.toXContent(builder, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return synonymSet.equals(response.synonymSet);
        }

        @Override
        public int hashCode() {
            return Objects.hash(synonymSet);
        }
    }
}
