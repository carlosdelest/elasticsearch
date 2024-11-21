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

package co.elastic.elasticsearch.metering.activitytracking;

/**
 * Classification for actions which describes how they will be tracked.
 */
public enum ActionTier {

    /**
     * Only track action as a search activity.
     */
    SEARCH,

    /**
     * Only track action as an index activity.
     */
    INDEX,

    /**
     * Track action as both a search and an index activity.
     */
    BOTH,

    /**
     * Do not track the action.
     */
    NEITHER;

    interface Mapper {
        ActionTier toTier(String action);
    }

    record Rule(String actionPrefix, ActionTier tier) {};
}
