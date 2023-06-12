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

package co.elastic.elasticsearch.stateless.xpack;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

/**
 * A dummy version of xpack info for searchable snapshots.
 *
 * Serverless does not contain searchable snapshots code. This class temporarily bridges
 * the expectation of the xpack info infrastructure which expects all xpack feature infos
 * to be available.
 */
public class DummySearchableSnapshotsInfoTransportAction extends XPackInfoFeatureTransportAction {

    @Inject
    public DummySearchableSnapshotsInfoTransportAction(TransportService transportService, ActionFilters actionFilters) {
        super(XPackInfoFeatureAction.SEARCHABLE_SNAPSHOTS.name(), transportService, actionFilters);
    }

    @Override
    protected String name() {
        return XPackInfoFeatureAction.SEARCHABLE_SNAPSHOTS.name();
    }

    @Override
    protected boolean available() {
        return false;
    }

    @Override
    protected boolean enabled() {
        return false;
    }
}
