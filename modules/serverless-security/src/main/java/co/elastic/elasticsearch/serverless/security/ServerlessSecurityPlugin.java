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

package co.elastic.elasticsearch.serverless.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings.EXCLUDE_ROLES;
import static org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore.INCLUDED_RESERVED_ROLES_SETTING;
import static org.elasticsearch.xpack.security.operator.OperatorPrivileges.OPERATOR_PRIVILEGES_ENABLED;

/**
 * Custom rules for security, e.g. operator privileges and reserved roles, when running in the serverless environment.
 */
public class ServerlessSecurityPlugin extends Plugin implements ActionPlugin {

    private static final Logger logger = LogManager.getLogger(ServerlessSecurityPlugin.class);

    /**
     * Register a <code>Setting</code> for the (x-pack) "native_users.enabled" implicit setting.
     * X-Pack never registers the setting - it only exists so that serverless can use it.
     * We default to disabled, but allow it to be enabled again for automated tests
     * (this allows us to share many test cases between stateful and serverless)
     */
    public static final Setting<Boolean> NATIVE_USERS_SETTING = Setting.boolSetting(
        NativeRealmSettings.NATIVE_USERS_ENABLED,
        false,
        Setting.Property.NodeScope
    );

    /**
     * Register a <code>Setting</code> for the (x-pack) "native_roles.enabled" implicit setting.
     * @see #NATIVE_USERS_SETTING
     */
    public static final Setting<Boolean> NATIVE_ROLES_SETTING = Setting.boolSetting(
        "xpack.security.authc.native_roles.enabled",
        true,
        Setting.Property.NodeScope
    );

    /**
     * Registers the setting for enabling the {@code ClusterStateRoleMapper} from the x-pack Security plugin.
     * The serverless Security plugin enables the {@code ClusterStateRoleMapper} because the
     * {@code ReservedRoleMappingAction} (also from the x-pack Security plugin) uses it to enact the role
     * mappings from the `settings.json` file.
     */
    public static final Setting<Boolean> CLUSTER_STATE_ROLE_MAPPINGS_ENABLED_SETTING = Setting.boolSetting(
        "xpack.security.authc.cluster_state_role_mappings.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * Registers the setting for disabling the {@code NativeRoleMappingStore} from the x-pack Security plugin.
     * The serverless Security plugin disables the {@code NativeRoleMappingStore} because the
     * the role mapping endpoints, which store the role mappings in the .security index,
     * are not used in serverless projects, and instead it's the cluster state-based role mappings,
     * i.e. {@code ClusterStateRoleMapper}, that are used.
     */
    public static final Setting<Boolean> NATIVE_ROLE_MAPPINGS_ENABLED_SETTING = Setting.boolSetting(
        "xpack.security.authc.native_role_mappings.enabled",
        true,
        Setting.Property.NodeScope
    );

    private final AtomicReference<SecurityContext> securityContext = new AtomicReference<>();

    @Override
    public Collection<?> createComponents(PluginServices services) {
        if (OPERATOR_PRIVILEGES_ENABLED.get(services.environment().settings()) == false) {
            throw new IllegalStateException(
                "Operator privileges are required for serverless deployments. Please set ["
                    + OPERATOR_PRIVILEGES_ENABLED.getKey()
                    + "] to true"
            );
        }

        this.securityContext.set(new SecurityContext(services.environment().settings(), services.threadPool().getThreadContext()));

        return Collections.emptyList();
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
            .putList(INCLUDED_RESERVED_ROLES_SETTING.getKey(), "superuser", "remote_monitoring_agent", "remote_monitoring_collector")
            .put(OPERATOR_PRIVILEGES_ENABLED.getKey(), true)
            .put(NATIVE_USERS_SETTING.getKey(), false)
            .put(NATIVE_ROLES_SETTING.getKey(), true)
            .put(CLUSTER_STATE_ROLE_MAPPINGS_ENABLED_SETTING.getKey(), true) // the setting is false by default; this sets it to true
            .put(NATIVE_ROLE_MAPPINGS_ENABLED_SETTING.getKey(), false) // the setting is true by default; this sets it to false
            .build();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            INCLUDED_RESERVED_ROLES_SETTING,
            NATIVE_USERS_SETTING,
            NATIVE_ROLES_SETTING,
            CLUSTER_STATE_ROLE_MAPPINGS_ENABLED_SETTING,
            NATIVE_ROLE_MAPPINGS_ENABLED_SETTING,
            EXCLUDE_ROLES
        );
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        Settings settings,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        restController.getApiProtections().setEnabled(true);
        return List.of();
    }

    public SecurityContext getSecurityContext() {
        return securityContext.get();
    }
}
