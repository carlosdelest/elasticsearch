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

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;

import java.util.concurrent.atomic.AtomicLong;

public class IngestLoadPublisher {
    private final NodeClient client;
    private final AtomicLong seqNoSupplier = new AtomicLong();

    public IngestLoadPublisher(Client client) {
        this.client = (NodeClient) client;
    }

    public void publishIngestionLoad(double ingestionLoad, String nodeId, ActionListener<Void> listener) {
        var request = new PublishNodeIngestLoadRequest(nodeId, seqNoSupplier.incrementAndGet(), ingestionLoad);
        client.execute(PublishNodeIngestLoadAction.INSTANCE, request, listener.map(unused -> null));
    }
}
