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
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class DeleteSynonymsAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteSynonymsAction INSTANCE = new DeleteSynonymsAction();
    public static final String NAME = "cluster:admin/synonyms/delete";

    public DeleteSynonymsAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends ActionRequest {
        private final String resourceName;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.resourceName = in.readString();
        }

        public Request(String resourceName) throws IOException {
            Objects.requireNonNull(resourceName, "resource name must be specified");
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
    }
}
