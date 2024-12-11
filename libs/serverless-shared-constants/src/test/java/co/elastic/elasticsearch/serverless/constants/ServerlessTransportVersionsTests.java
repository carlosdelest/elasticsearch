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
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ServerlessTransportVersionsTests extends ESTestCase {

    public void testIdsOnlyChangeServerlessPart() {
        var serverVersions = TransportVersion.getAllVersions();
        for (var serverlessVersion : ServerlessTransportVersions.DEFINED_VERSIONS) {
            var id = serverlessVersion.id();
            var serverlessPart = id % 1000 / 10; // isolate serverless part of version id
            assertThat("serverless part must must be non-zero for transport version " + serverlessVersion, serverlessPart, not(0));
            var upstreamId = id / 1000 * 1000;
            assertThat(
                "Serverless transport version " + serverlessVersion + " must be based on a transport version from server",
                Collections.binarySearch(serverVersions, new TransportVersion(upstreamId)),
                is(not(-1))
            );
        }
    }

    public void testAllVersionsIncludeServerless() {
        List<TransportVersion> versions = TransportVersion.getAllVersions();

        assertThat(ServerlessTransportVersions.DEFINED_VERSIONS, everyItem(is(in(new HashSet<>(versions)))));
    }
}
