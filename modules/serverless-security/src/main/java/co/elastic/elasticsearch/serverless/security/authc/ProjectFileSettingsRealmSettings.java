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

package co.elastic.elasticsearch.serverless.security.authc;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.realmSettingPrefix;

public final class ProjectFileSettingsRealmSettings {
    public static final String TYPE = "project_file_settings";
    private static final Set<String> SUPPORTED_USERS = Set.of("admin", "testing-internal");

    public static final Function<String, Setting.AffixSetting<SecureString>> PROJECT_FILE_SETTINGS_PASSWORD_HASH = user -> Setting
        .affixKeySetting(realmSettingPrefix(TYPE), user + ".password_hash", key -> SecureSetting.secureString(key, null));

    public static final Function<String, Setting.AffixSetting<List<String>>> PROJECT_FILE_SETTINGS_ROLES = user -> Setting.affixKeySetting(
        realmSettingPrefix(TYPE),
        user + ".roles",
        key -> Setting.stringListSetting(key, Setting.Property.NodeScope)
    );

    public static final Map<String, Setting.AffixSetting<SecureString>> HASH_SETTING_BY_USER = SUPPORTED_USERS.stream()
        .collect(Collectors.toMap(Function.identity(), PROJECT_FILE_SETTINGS_PASSWORD_HASH));

    public static final Map<String, Setting.AffixSetting<List<String>>> ROLES_SETTING_BY_USER = SUPPORTED_USERS.stream()
        .collect(Collectors.toMap(Function.identity(), PROJECT_FILE_SETTINGS_ROLES));

    public static final Setting.AffixSetting<String> CACHE_HASH_ALGO_SETTING = RealmSettings.affixSetting(
        "cache.hash_algo",
        key -> Setting.simpleString(key, "ssha256", Setting.Property.NodeScope)
    ).apply(TYPE);

    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueHours(1);
    public static final Setting.AffixSetting<TimeValue> CACHE_TTL_SETTING = RealmSettings.affixSetting(
        "cache.ttl",
        key -> Setting.timeSetting(key, DEFAULT_TTL, Setting.Property.NodeScope)
    ).apply(TYPE);
    private static final int DEFAULT_MAX_SIZE = 1000;
    public static final Setting.AffixSetting<Integer> CACHE_MAX_SIZE_SETTING = RealmSettings.affixSetting(
        "cache.max_size",
        key -> Setting.intSetting(key, DEFAULT_MAX_SIZE, Setting.Property.NodeScope)
    ).apply(TYPE);

    private ProjectFileSettingsRealmSettings() {}

    /**
     * @return The {@link Setting setting configuration} for this realm type
     */
    public static Set<Setting.AffixSetting<?>> getSettings() {
        final Set<Setting.AffixSetting<?>> settings = new HashSet<>(CachingUsernamePasswordRealmSettings.getSettings(TYPE));
        settings.addAll(RealmSettings.getStandardSettings(TYPE));
        settings.addAll(ROLES_SETTING_BY_USER.values());
        settings.add(CACHE_MAX_SIZE_SETTING);
        settings.add(CACHE_TTL_SETTING);
        settings.add(CACHE_HASH_ALGO_SETTING);
        return settings;
    }
}
