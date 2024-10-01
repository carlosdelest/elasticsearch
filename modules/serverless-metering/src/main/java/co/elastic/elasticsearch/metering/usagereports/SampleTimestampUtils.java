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

package co.elastic.elasticsearch.metering.usagereports;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

class SampleTimestampUtils {

    static Instant calculateSampleTimestamp(Instant now, Duration reportPeriod) {
        // this essentially calculates 'now' mod the reportPeriod, relative to hour timeslots
        // this gets us a consistent rounded report period, regardless of where in that period
        // the record is actually being calculated
        assert reportPeriod.compareTo(Duration.ofHours(1)) <= 0;
        assert reportPeriod.compareTo(Duration.ofSeconds(1)) >= 0;

        // round to the hour as a baseline
        Instant hour = now.truncatedTo(ChronoUnit.HOURS);
        // get the time into the hour we are
        Duration intoHour = Duration.between(hour, now);
        // get how many times reportPeriod divides into the duration
        long times = intoHour.dividedBy(reportPeriod);
        // get our floor'd timestamp
        return hour.plus(reportPeriod.multipliedBy(times));
    }
}
