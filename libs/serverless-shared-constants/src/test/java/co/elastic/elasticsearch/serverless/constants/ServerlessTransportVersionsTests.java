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

import java.util.Set;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class ServerlessTransportVersionsTests extends ESTestCase {

    public void testIdsOnlyChangeServerlessPart() {
        Set<TransportVersion> serverVersions = Set.copyOf(TransportVersion.getAllVersions());
        for (var serverlessVersion : ServerlessTransportVersions.DEFINED_VERSIONS) {
            int id = serverlessVersion.id();
            int serverlessPart = id % 1000 / 100; // isolate serverless part of version id
            assertThat("serverless part must must be non-zero for transport version " + serverlessVersion, serverlessPart, not(0));
            int upstreamId = id / 1000 * 1000;
            assertThat(
                "Serverless transport version " + serverlessVersion + " must be based on a transport version from server",
                new TransportVersion(upstreamId),
                in(serverVersions)
            );
        }
    }

    public void testServerlessVersionsStillAvailable() {
        for (TransportVersion serverlessVersion : ServerlessTransportVersions.DEFINED_VERSIONS) {
            if (serverlessVersion.id() % 1000 >= 900) {
                fail(
                    "There are no more Serverless versions available beyond "
                        + serverlessVersion
                        + ". Please inform the Core/Infra team to determine remediation steps."
                );
            }
        }
    }

    public void testAllVersionsIncludeServerless() {
        assertThat(ServerlessTransportVersions.DEFINED_VERSIONS, everyItem(is(in(TransportVersion.getAllVersions()))));
    }
}
