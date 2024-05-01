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
import java.util.concurrent.atomic.AtomicBoolean;
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
        false,
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

    public static final Setting<Boolean> API_KEY_STRICT_REQUEST_VALIDATION = Setting.boolSetting(
        "xpack.security.authc.api_key.strict_request_validation.enabled",
        true,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> HAS_PRIVILEGES_STRICT_REQUEST_VALIDATION = Setting.boolSetting(
        "xpack.security.authz.has_privileges.strict_request_validation.enabled",
        false, // TODO : This will become true at a later time
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    private AtomicBoolean apiKeyStrictRequestValidation;
    private AtomicBoolean hasPrivilegesStrictRequestValidation;

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
        final ClusterSettings clusterSettings = services.clusterService().getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(API_KEY_STRICT_REQUEST_VALIDATION, this::configureStrictApiKeyRequestValidation);
        clusterSettings.addSettingsUpdateConsumer(
            HAS_PRIVILEGES_STRICT_REQUEST_VALIDATION,
            this::configureStrictHasPrivilegesRequestValidation
        );

        this.securityContext.set(new SecurityContext(services.environment().settings(), services.threadPool().getThreadContext()));

        return Collections.emptyList();
    }

    private void configureStrictApiKeyRequestValidation(boolean enabled) {
        if (this.apiKeyStrictRequestValidation == null) {
            throw new IllegalStateException("Strict API key request validation object not configured");
        } else {
            this.apiKeyStrictRequestValidation.set(enabled);
        }
    }

    private void configureStrictHasPrivilegesRequestValidation(boolean enabled) {
        if (this.hasPrivilegesStrictRequestValidation == null) {
            throw new IllegalStateException("Strict Has Privileges request validation object not configured");
        } else {
            this.hasPrivilegesStrictRequestValidation.set(enabled);
        }
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
            .putList(INCLUDED_RESERVED_ROLES_SETTING.getKey(), "superuser", "remote_monitoring_agent", "remote_monitoring_collector")
            .put(OPERATOR_PRIVILEGES_ENABLED.getKey(), true)
            .put(NATIVE_USERS_SETTING.getKey(), false)
            .put(NATIVE_ROLES_SETTING.getKey(), false)
            .put(CLUSTER_STATE_ROLE_MAPPINGS_ENABLED_SETTING.getKey(), true) // the setting is false by default; this sets it to true
            .put(API_KEY_STRICT_REQUEST_VALIDATION.getKey(), true)
            .put(HAS_PRIVILEGES_STRICT_REQUEST_VALIDATION.getKey(), false)
            .build();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            INCLUDED_RESERVED_ROLES_SETTING,
            NATIVE_USERS_SETTING,
            NATIVE_ROLES_SETTING,
            CLUSTER_STATE_ROLE_MAPPINGS_ENABLED_SETTING,
            EXCLUDE_ROLES,
            API_KEY_STRICT_REQUEST_VALIDATION,
            HAS_PRIVILEGES_STRICT_REQUEST_VALIDATION
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
        this.apiKeyStrictRequestValidation = new AtomicBoolean(clusterSettings.get(API_KEY_STRICT_REQUEST_VALIDATION));
        this.hasPrivilegesStrictRequestValidation = new AtomicBoolean(clusterSettings.get(HAS_PRIVILEGES_STRICT_REQUEST_VALIDATION));
        restController.getApiProtections().setEnabled(true);
        return List.of();
    }

    public boolean strictApiKeyRequestValidationEnabled() {
        return apiKeyStrictRequestValidation != null && apiKeyStrictRequestValidation.get();
    }

    public boolean strictHasPrivilegesRequestValidationEnabled() {
        return hasPrivilegesStrictRequestValidation != null && hasPrivilegesStrictRequestValidation.get();
    }

    public SecurityContext getSecurityContext() {
        return securityContext.get();
    }
}
