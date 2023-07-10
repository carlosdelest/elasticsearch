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

package org.elasticsearch.test.cluster.serverless.remote;

import java.util.Optional;

public class DefaultServerlessClusterAccessProvider implements ServerlessClusterAccessProvider {

    @Override
    public Optional<String> getHttpEndpoint() {
        String essPublicUrl = System.getenv().get("ESS_PUBLIC_URL");
        if (essPublicUrl != null) {
            String httpEndpoint = essPublicUrl.replace("https://", "") + ":443";
            return Optional.of(httpEndpoint);
        } else {
            return Optional.empty();
        }
    }
}
