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

package co.elastic.elasticsearch.metering;

/**
 * Usage metadata keys used in usage records for metering.
 */
public interface UsageMetadata {
    String APPLICATION_TIER = "application_tier";

    String ACTIVE = "active";
    String LATEST_ACTIVITY_TIMESTAMP = "latest_activity_timestamp";

    String SEARCH_TIER_ACTIVE = "search_tier_active";
    String SEARCH_TIER_LATEST_ACTIVITY_TIMESTAMP = "search_tier_latest_activity_timestamp";

    String SP_MIN = "sp_min";
    String SP_MIN_PROVISIONED_MEMORY = "sp_min_provisioned_memory";
    String SP_MIN_STORAGE_RAM_RATIO = "sp_min_storage_ram_ratio";
}
