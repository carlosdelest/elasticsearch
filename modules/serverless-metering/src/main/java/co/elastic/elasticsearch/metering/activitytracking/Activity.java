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

import java.time.Instant;

public record Activity(Instant lastActivityRecentPeriod, Instant firstActivityRecentPeriod, Instant lastActivityPreviousPeriod) {

    public static Activity EMPTY = new Activity(Instant.EPOCH, Instant.EPOCH, Instant.EPOCH);

    Activity extendCurrentPeriod(Instant now) {
        var newLastActivity = now.isAfter(lastActivityRecentPeriod) ? now : lastActivityRecentPeriod;
        return new Activity(newLastActivity, firstActivityRecentPeriod, lastActivityPreviousPeriod);
    }

    Activity makeNewPeriod(Instant now) {
        return new Activity(now, now, lastActivityRecentPeriod);
    }
};
