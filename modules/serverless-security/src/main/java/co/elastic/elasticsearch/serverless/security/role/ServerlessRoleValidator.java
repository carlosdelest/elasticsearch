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

package co.elastic.elasticsearch.serverless.security.role;

import co.elastic.elasticsearch.serverless.security.privilege.ServerlessSupportedPrivilegesRegistry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.action.role.RoleDescriptorRequestValidator;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.restriction.WorkflowResolver;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.security.authz.FileRoleValidator;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Contains the logic used for validating roles, both Custom (user-provided) and Predefined (Elastic-provided via File config).
 */
public final class ServerlessRoleValidator implements FileRoleValidator {
    Logger logger = LogManager.getLogger(ServerlessRoleValidator.class);

    // package-private for testing
    static final Set<String> SUPPORTED_APPLICATION_NAMES = Set.of("apm", "fleet", "kibana-.kibana");
    static final String RESERVED_ROLE_NAME_PREFIX = "_";
    static final String PUBLIC_METADATA_KEY = MetadataUtils.RESERVED_PREFIX + "public";
    static final Set<String> PREDEFINED_ROLE_METADATA_ALLOWLIST = Set.of(PUBLIC_METADATA_KEY);

    public ServerlessRoleValidator() {}

    public ActionRequestValidationException validateCustomRole(RoleDescriptor roleDescriptor) {
        return validateCustomRole(roleDescriptor, true);
    }

    public void validateCustomRoleAndThrow(@Nullable List<RoleDescriptor> roleDescriptors, boolean validateRoleName) {
        if (roleDescriptors == null || roleDescriptors.isEmpty()) {
            return;
        }
        ActionRequestValidationException validationException = null;
        for (var roleDescriptor : roleDescriptors) {
            validationException = validateCustomRole(roleDescriptor, validateRoleName, validationException);
        }
        if (validationException != null) {
            throw validationException;
        }
    }

    public ActionRequestValidationException validateCustomRole(
        RoleDescriptor roleDescriptor,
        boolean validateRoleName,
        ActionRequestValidationException validationException
    ) {
        if (validateRoleName) {
            validationException = validateRoleName(roleDescriptor.getName(), validationException);
        }
        validationException = validateRoleDescriptor(roleDescriptor, false, validationException);
        // Serverless validation is stricter than regular role descriptor validation, therefore the invariant that a valid custom
        // role is also a valid "regular" role must hold
        assert validationException != null || RoleDescriptorRequestValidator.validate(roleDescriptor) == null;
        return validationException;
    }

    public ActionRequestValidationException validateCustomRole(RoleDescriptor roleDescriptor, boolean validateRoleName) {
        return validateCustomRole(roleDescriptor, validateRoleName, null);
    }

    /**
     * Validate a predefined role. Predefined roles are only subject to the same validation if they are public roles, as defined by
     * metadata in the role definition. They are also allowed to have that metadata key, which custom roles are not.
     * @param roleDescriptor The predefined role to validate.
     * @return {@code null} if there are no problems with the role, or an exception describing the problems otherwise.
     */
    @Override
    public ActionRequestValidationException validatePredefinedRole(RoleDescriptor roleDescriptor) {
        if (isMarkedPublic(roleDescriptor)) {
            ActionRequestValidationException ex = null;

            if (roleDescriptor.hasConfigurableClusterPrivileges()) {
                ex = addValidationError("Serverless predefined roles may not have configurable cluster privileges", ex);
            }
            if (roleDescriptor.hasRemoteIndicesPrivileges()) {
                ex = addValidationError("Serverless predefined roles may not have remote indices privileges", ex);
            }
            if (roleDescriptor.hasRunAs()) {
                ex = addValidationError("Serverless predefined roles may not have run-as privileges", ex);
            }
            if (roleDescriptor.hasWorkflowsRestriction()) {
                ex = addValidationError("Serverless predefined roles may not have workflow restrictions", ex);
            }

            if (ex != null) {
                throw ex;
            }
            return validateRoleDescriptor(roleDescriptor, true, null);
        } else {
            logger.debug(LoggerMessageFormat.format("skipping role validation for non-public role [{}]", roleDescriptor.getName()));
            return null;
        }
    }

    private ActionRequestValidationException validateRoleName(String roleName, ActionRequestValidationException validationException) {
        final Validation.Error error = NativeRealmValidationUtil.validateRoleName(roleName, false);
        if (error != null) {
            validationException = addValidationError(error.toString(), validationException);
        } else if (roleName.startsWith(RESERVED_ROLE_NAME_PREFIX)) {
            validationException = addValidationError(
                "role name may not start with [" + RESERVED_ROLE_NAME_PREFIX + "]",
                validationException
            );
        }
        return validationException;
    }

    private ActionRequestValidationException validateRoleDescriptor(
        RoleDescriptor roleDescriptor,
        boolean allowKnownReservedMetadata,
        ActionRequestValidationException validationException
    ) {
        assert roleDescriptor.getName() != null;
        assert false == roleDescriptor.hasConfigurableClusterPrivileges();
        assert false == roleDescriptor.hasRemoteIndicesPrivileges();
        assert false == roleDescriptor.hasRunAs();
        if (roleDescriptor.getClusterPrivileges() != null) {
            for (String cp : roleDescriptor.getClusterPrivileges()) {
                validationException = validateClusterPrivilege(cp, validationException);
            }
        }
        if (roleDescriptor.getIndicesPrivileges() != null) {
            for (RoleDescriptor.IndicesPrivileges idp : roleDescriptor.getIndicesPrivileges()) {
                validationException = validateIndicesPrivileges(idp, validationException);
            }
        }
        if (roleDescriptor.getApplicationPrivileges() != null) {
            for (RoleDescriptor.ApplicationResourcePrivileges privilege : roleDescriptor.getApplicationPrivileges()) {
                validationException = validateApplicationPrivileges(privilege, validationException);
            }
        }
        Map<String, Object> metadata = roleDescriptor.getMetadata();
        if (metadata != null) {
            Set<String> disallowedKeys = metadata.keySet()
                .stream()
                .filter(key -> key.startsWith(MetadataUtils.RESERVED_PREFIX))
                .filter(key -> (allowKnownReservedMetadata && PREDEFINED_ROLE_METADATA_ALLOWLIST.contains(key)) == false)
                .collect(Collectors.toSet());
            if (disallowedKeys.isEmpty() == false) {
                validationException = addValidationError(
                    "role descriptor metadata keys may not start with ["
                        + MetadataUtils.RESERVED_PREFIX
                        + "] but found these keys: "
                        + disallowedKeys,
                    validationException
                );
            }
        }
        if (roleDescriptor.hasWorkflowsRestriction()) {
            for (String workflowName : roleDescriptor.getRestriction().getWorkflows()) {
                try {
                    WorkflowResolver.resolveWorkflowByName(workflowName);
                } catch (IllegalArgumentException e) {
                    validationException = addValidationError(e.getMessage(), validationException);
                }
            }
        }
        if (roleDescriptor.hasDescription()) {
            Validation.Error error = Validation.Roles.validateRoleDescription(roleDescriptor.getDescription());
            if (error != null) {
                validationException = addValidationError(error.toString(), validationException);
            }
        }
        return validationException;
    }

    private boolean isMarkedPublic(RoleDescriptor roleDescriptor) {
        return Optional.ofNullable(roleDescriptor)
            .map(descriptor -> descriptor.getMetadata())
            .map(metadata -> metadata.get(PUBLIC_METADATA_KEY))
            .map(Boolean.TRUE::equals)
            .orElse(false);
    }

    private ActionRequestValidationException validateClusterPrivilege(
        String clusterPrivilege,
        ActionRequestValidationException validationException
    ) {
        if (ClusterPrivilegeResolver.getNamedOrNull(clusterPrivilege) == null) {
            validationException = addValidationError(
                "unknown cluster privilege [" + clusterPrivilege + "]. " + mustBePredefinedClusterPrivilegeMessage(),
                validationException
            );
        } else if (false == ServerlessSupportedPrivilegesRegistry.isSupportedClusterPrivilege(clusterPrivilege)) {
            validationException = addValidationError(
                "cluster privilege ["
                    + clusterPrivilege
                    + "] exists but is not supported when running in serverless mode. "
                    + mustBePredefinedClusterPrivilegeMessage(),
                validationException
            );
        }
        return validationException;
    }

    public static String mustBePredefinedClusterPrivilegeMessage() {
        return "a privilege must be one of the predefined cluster privilege names ["
            + Strings.collectionToCommaDelimitedString(ServerlessSupportedPrivilegesRegistry.supportedClusterPrivilegeNames())
            + "]";
    }

    private ActionRequestValidationException validateIndicesPrivileges(
        RoleDescriptor.IndicesPrivileges indicesPrivileges,
        ActionRequestValidationException validationException
    ) {
        if (indicesPrivileges.allowRestrictedIndices()) {
            validationException = addValidationError(
                "access to restricted indices is not supported when running in serverless mode",
                validationException
            );
        }
        for (String privilegeName : indicesPrivileges.getPrivileges()) {
            if (IndexPrivilege.getNamedOrNull(privilegeName) == null) {
                validationException = addValidationError(
                    "unknown index privilege [" + privilegeName + "]. " + mustBePredefinedIndexPrivilegeMessage(),
                    validationException
                );
            } else if (false == ServerlessSupportedPrivilegesRegistry.isSupportedIndexPrivilege(privilegeName)) {
                validationException = addValidationError(
                    "index privilege ["
                        + privilegeName
                        + "] exists but is not supported when running in serverless mode. "
                        + mustBePredefinedIndexPrivilegeMessage(),
                    validationException
                );
            }
        }
        return validationException;
    }

    public static String mustBePredefinedIndexPrivilegeMessage() {
        return "a privilege must be one of the predefined index privilege names ["
            + Strings.collectionToCommaDelimitedString(ServerlessSupportedPrivilegesRegistry.supportedIndexPrivilegeNames())
            + "]";
    }

    private ActionRequestValidationException validateApplicationPrivileges(
        RoleDescriptor.ApplicationResourcePrivileges applicationPrivileges,
        ActionRequestValidationException validationException
    ) {
        final String applicationName = applicationPrivileges.getApplication();
        // plain wildcard is supported; otherwise, it must be one of the supported application names
        if (false == (isWildcard(applicationName) || SUPPORTED_APPLICATION_NAMES.contains(applicationName))) {
            validationException = addValidationError(
                "invalid application name ["
                    + applicationName
                    + "]. name must be a wildcard [*] or one of the supported application names ["
                    + Strings.collectionToCommaDelimitedString(new TreeSet<>(SUPPORTED_APPLICATION_NAMES))
                    + "]",
                validationException
            );
        }
        for (String privilegeName : applicationPrivileges.getPrivileges()) {
            try {
                ApplicationPrivilege.validatePrivilegeOrActionName(privilegeName);
            } catch (IllegalArgumentException e) {
                validationException = addValidationError(e.getMessage(), validationException);
            }
        }
        return validationException;
    }

    private boolean isWildcard(String pattern) {
        return pattern.equals("*");
    }
}
