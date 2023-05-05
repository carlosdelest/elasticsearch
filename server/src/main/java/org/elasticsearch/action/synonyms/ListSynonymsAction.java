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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.synonyms.SynonymSetList;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ListSynonymsAction extends ActionType<ListSynonymsAction.Response> {

    public static final ListSynonymsAction INSTANCE = new ListSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/list";

    public ListSynonymsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request() throws IOException {}

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final SynonymSetList itemsList;

        public Response(StreamInput in) throws IOException {
            super(in);
            itemsList = new SynonymSetList(in);
        }

        public Response(SynonymSetList itemsList) {
            Objects.requireNonNull(itemsList, "list items must be specified");
            this.itemsList = itemsList;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            itemsList.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            itemsList.toXContent(builder, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return itemsList.equals(response.itemsList);
        }

        @Override
        public int hashCode() {
            return Objects.hash(itemsList);
        }
    }
}
