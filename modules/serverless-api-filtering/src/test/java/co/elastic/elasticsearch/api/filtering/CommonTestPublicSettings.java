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

package co.elastic.elasticsearch.api.filtering;

import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.List;
import java.util.Set;

public class CommonTestPublicSettings {
    public static Setting<Integer> PUBLIC_SETTING = Setting.intSetting(
        "index.public_setting",
        1,
        0,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope,
        Setting.Property.ServerlessPublic
    );

    public static Setting<Integer> NON_PUBLIC_SETTING = Setting.intSetting(
        "index.internal_setting",
        1,
        0,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );
    public static ThreadContext THREAD_CONTEXT = new ThreadContext(Settings.EMPTY);

    public static final Setting.AffixSetting<List<String>> AFFIX_PUBLIC_SETTING = Setting.prefixKeySetting(
        "index.public.affix.",
        key -> Setting.stringListSetting(key, Setting.Property.Dynamic, Setting.Property.IndexScope, Setting.Property.ServerlessPublic)
    );

    public static final Setting.AffixSetting<List<String>> AFFIX_PRIVATE_SETTING = Setting.prefixKeySetting(
        "index.private.affix.",
        key -> Setting.stringListSetting(key, Setting.Property.Dynamic, Setting.Property.IndexScope)
    );

    public static IndexScopedSettings MIXED_PUBLIC_NON_PUBLIC_INDEX_SCOPED_SETTINGS = new IndexScopedSettings(
        Settings.EMPTY,
        Set.of(PUBLIC_SETTING, NON_PUBLIC_SETTING, AFFIX_PRIVATE_SETTING, AFFIX_PUBLIC_SETTING)
    );
}
