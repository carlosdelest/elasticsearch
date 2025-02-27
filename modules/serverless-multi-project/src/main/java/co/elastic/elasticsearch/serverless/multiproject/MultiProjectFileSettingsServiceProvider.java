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

package co.elastic.elasticsearch.serverless.multiproject;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.reservedstate.service.FileSettingsService;
import org.elasticsearch.reservedstate.service.FileSettingsServiceProvider;
import org.elasticsearch.reservedstate.service.ReservedClusterStateService;

public class MultiProjectFileSettingsServiceProvider implements FileSettingsServiceProvider {
    @Override
    public FileSettingsService construct(
        ClusterService clusterService,
        ReservedClusterStateService stateService,
        Environment environment,
        FileSettingsService.FileSettingsHealthIndicatorService healthIndicatorService
    ) {
        return new MultiProjectFileSettingsService(clusterService, stateService, environment, healthIndicatorService);
    }
}
