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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccount;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class ProjectServiceAccountTokenStoreSettings {

    private static final String SETTINGS_PREFIX = "xpack.security.authc.project_service_token";

    private static final Function<
        ServiceAccount.ServiceAccountId,
        Setting.AffixSetting<SecureString>> SERVICE_TOKEN_HASH_SETTING_GENERATOR = (serviceAccountId) -> Setting.prefixKeySetting(
            Strings.format("%s.%s.%s.", SETTINGS_PREFIX, serviceAccountId.namespace(), serviceAccountId.serviceName()),
            key -> SecureSetting.secureString(key, null)
        );

    public static final Map<ServiceAccount.ServiceAccountId, Setting.AffixSetting<SecureString>> SERVICE_TOKEN_HASH_SETTING_BY_ACCOUNT_ID =
        ServiceAccountService.getServiceAccounts()
            .values()
            .stream()
            .collect(Collectors.toMap(ServiceAccount::id, account -> SERVICE_TOKEN_HASH_SETTING_GENERATOR.apply(account.id())));

    public static final Setting<String> CACHE_HASH_ALGO_SETTING = Setting.simpleString(
        SETTINGS_PREFIX + ".cache.hash_algo",
        "ssha256",
        Setting.Property.NodeScope
    );

    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueHours(1);
    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting(
        SETTINGS_PREFIX + ".cache.ttl",
        DEFAULT_TTL,
        Setting.Property.NodeScope
    );
    private static final int DEFAULT_MAX_SIZE = 1000;
    public static final Setting<Integer> CACHE_MAX_SIZE_SETTING = Setting.intSetting(
        SETTINGS_PREFIX + ".cache.max_size",
        DEFAULT_MAX_SIZE,
        Setting.Property.NodeScope
    );

    private ProjectServiceAccountTokenStoreSettings() {}

    public static Set<Setting<?>> getSettings() {
        HashSet<Setting<?>> settings = new HashSet<>(SERVICE_TOKEN_HASH_SETTING_BY_ACCOUNT_ID.values());
        settings.add(CACHE_HASH_ALGO_SETTING);
        settings.add(CACHE_TTL_SETTING);
        settings.add(CACHE_MAX_SIZE_SETTING);
        return settings;
    }
}
