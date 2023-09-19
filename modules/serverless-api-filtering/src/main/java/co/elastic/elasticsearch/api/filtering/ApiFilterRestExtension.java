/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.api.filtering;

import org.elasticsearch.plugins.internal.RestExtension;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.action.cat.AbstractCatAction;

import java.util.function.Predicate;

public class ApiFilterRestExtension implements RestExtension {

    @Override
    public Predicate<AbstractCatAction> getCatActionsFilter() {
        // these are the cat actions returned in /_cat. We don't need this api as operators,
        // so we don't mess with trying to return internal apis when in operator only mode
        return handler -> handler.getServerlessScope() == Scope.PUBLIC;
    }

    @Override
    public Predicate<RestHandler> getActionsFilter() {
        // All we care about here is there is some serverless scope. The distinction between
        // public and internal is handled at routing time by the RestController and security
        return handler -> handler.getServerlessScope() != null;
    }
}
