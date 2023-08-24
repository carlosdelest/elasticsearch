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

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;

/**
 * Settings that may be read across multiple serverless modules.
 *
 * These setting constants must be only registered once, so each must
 * have an "owning" module that registers the setting object.
 */
public class ServerlessSharedSettings {

    public static final Setting<TimeValue> BOOST_WINDOW_SETTING = Setting.timeSetting(
        "serverless.search.boost_window",
        TimeValue.timeValueDays(7),
        TimeValue.timeValueDays(1),
        TimeValue.timeValueDays(365),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Integer> SEARCH_POWER_SETTING = Setting.intSetting(
        "serverless.search.search_power",
        100,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private ServerlessSharedSettings() {}
}
