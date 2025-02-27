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
package co.elastic.elasticsearch.serverless.security.authc.saml;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;

import java.util.Set;

import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.EXCLUDE_ROLES;

public class MultiProjectSpSamlRealmSettings {

    public static final String TYPE = "multi_project_saml";
    public static final String PREFIX = "xpack.security.authc.projects.";

    public static final Setting.AffixSetting<String> SP_ENTITY_ID = projectSetting("sp.entity_id");
    public static final Setting.AffixSetting<String> SP_ACS = projectSetting("sp.acs");
    public static final Setting.AffixSetting<String> SP_LOGOUT = projectSetting("sp.logout");

    private static Setting.AffixSetting<String> projectSetting(String suffix) {
        return Setting.affixKeySetting(
            PREFIX,
            suffix,
            key -> Setting.simpleString(key, Setting.Property.NodeScope, Setting.Property.Dynamic)
        );
    }

    public static <T> Setting<T> getProjectSetting(Setting.AffixSetting<T> setting, String projectId) {
        return setting.getConcreteSettingForNamespace(projectId);
    }

    public static Set<Setting.AffixSetting<?>> getSettings() {
        Set<Setting.AffixSetting<?>> samlSettings = SamlRealmSettings.getSettings(TYPE);
        samlSettings.add(SP_ENTITY_ID);
        samlSettings.add(SP_ACS);
        samlSettings.add(SP_LOGOUT);
        samlSettings.add(EXCLUDE_ROLES.apply(TYPE));

        return samlSettings;
    }
}
