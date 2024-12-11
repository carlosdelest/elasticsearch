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

package co.elastic.elasticsearch.serverless.constants;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.internal.VersionExtension;

import java.util.List;

public class ServerlessVersionExtension implements VersionExtension {
    @Override
    public List<TransportVersion> getTransportVersions() {
        return ServerlessTransportVersions.DEFINED_VERSIONS;
    }

    @Override
    public IndexVersion getCurrentIndexVersion(IndexVersion fallback) {
        return fallback;
    }
}
