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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static co.elastic.elasticsearch.serverless.security.role.ServerlessCustomRoleValidator.SUPPORTED_APPLICATION_NAMES;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ServerlessCustomRoleValidatorTests extends ESTestCase {

    @BeforeClass
    public static void init() {
        // necessary since the validator calls ReservedRolesStore::isReserved under the hood which is not available unless the class is
        // initialized
        new ReservedRolesStore();
    }

    public void testValidCustomRole() {
        final ServerlessCustomRoleValidator validator = new ServerlessCustomRoleValidator(ignored -> false);
        assertThat(validator.validate(randomRoleDescriptor()), is(nullValue()));
    }

    public void testInvalidCustomRole() {
        final String fileBasedName = randomAlphaOfLength(42);
        final ServerlessCustomRoleValidator validator = new ServerlessCustomRoleValidator(fileBasedName::equals);

        final int roleNameCaseNo = randomIntBetween(0, 3);
        final String roleName = switch (roleNameCaseNo) {
            case 0 -> randomAlphaOfLength(30); // valid
            case 1 -> fileBasedName; // file-based reserved
            case 2 -> "superuser"; // reserved
            case 3 -> "_" + randomAlphaOfLength(30); // invalid prefix
            default -> throw new IllegalStateException("Unexpected value: " + roleNameCaseNo);
        };
        final boolean invalidRoleName = roleNameCaseNo != 0;
        final boolean unknownClusterPrivilege = randomBoolean();
        final boolean unsupportedClusterPrivilege = randomBoolean();
        final boolean unknownIndexPrivilege = randomBoolean();
        final boolean unsupportedIndexPrivilege = randomBoolean();
        final boolean restrictedIndexAccess = randomBoolean();
        final boolean invalidApplicationName = randomBoolean();
        // ensure at least one validation error
        final boolean invalidApplicationPrivilege = false == (invalidRoleName
            && unknownClusterPrivilege
            && unsupportedClusterPrivilege
            && unknownIndexPrivilege
            && unsupportedIndexPrivilege
            && restrictedIndexAccess
            && invalidApplicationName) || randomBoolean();

        final List<String> clusterPrivileges = new ArrayList<>();
        if (unknownClusterPrivilege) {
            clusterPrivileges.add(randomValueOtherThanMany(ClusterPrivilegeResolver.names()::contains, () -> randomAlphaOfLength(10)));
        }
        if (unsupportedClusterPrivilege) {
            clusterPrivileges.add(
                randomValueOtherThanMany(
                    ServerlessSupportedPrivilegesRegistry.supportedClusterPrivilegeNames()::contains,
                    () -> randomFrom(ClusterPrivilegeResolver.names().toArray(new String[0]))
                )
            );
        }

        final List<String> indexPrivileges = new ArrayList<>();
        if (unknownIndexPrivilege) {
            indexPrivileges.add(randomValueOtherThanMany(IndexPrivilege.names()::contains, () -> randomAlphaOfLength(10)));
        }
        if (unsupportedIndexPrivilege) {
            indexPrivileges.add(
                randomValueOtherThanMany(
                    ServerlessSupportedPrivilegesRegistry.supportedIndexPrivilegeNames()::contains,
                    () -> randomFrom(IndexPrivilege.names().toArray(new String[0]))
                )
            );
        }
        if (restrictedIndexAccess) {
            indexPrivileges.add(randomFrom(ServerlessSupportedPrivilegesRegistry.supportedIndexPrivilegeNames().toArray(new String[0])));
        }
        final RoleDescriptor.IndicesPrivileges[] indicesPrivileges = indexPrivileges.isEmpty()
            ? null
            : new RoleDescriptor.IndicesPrivileges[] { indexBuilderWithPrivileges(indexPrivileges, restrictedIndexAccess, true).build() };

        final RoleDescriptor.ApplicationResourcePrivileges.Builder builder = RoleDescriptor.ApplicationResourcePrivileges.builder()
            .resources("*");
        if (invalidApplicationName) {
            builder.application(randomAlphaOfLength(32));
        } else {
            builder.application("*");
        }
        if (invalidApplicationPrivilege) {
            builder.privileges(" " + randomAlphaOfLength(4));
        } else {
            builder.privileges(generateRandomStringArray(6, randomIntBetween(4, 8), false, false));
        }
        final RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges = { builder.build() };

        final RoleDescriptor roleDescriptor = new RoleDescriptor(
            roleName,
            clusterPrivileges.toArray(String[]::new),
            indicesPrivileges,
            applicationPrivileges,
            null,
            null,
            Map.of(),
            Map.of(),
            null,
            null
        );

        final ActionRequestValidationException ex = validator.validate(roleDescriptor);
        assertThat(ex, is(notNullValue()));
        final List<String> validationErrors = ex.validationErrors();
        final List<Matcher<? super String>> itemMatchers = new ArrayList<>();
        switch (roleNameCaseNo) {
            case 1, 2 -> {
                itemMatchers.add(containsString("is reserved and may not be used"));
            }
            case 3 -> itemMatchers.add(containsString("role name may not start with [_]"));
        }
        if (unknownClusterPrivilege) {
            itemMatchers.add(
                allOf(
                    containsString("unknown cluster privilege"),
                    containsString(ServerlessCustomRoleValidator.mustBePredefinedClusterPrivilegeMessage())
                )
            );
        }
        if (unsupportedClusterPrivilege) {
            itemMatchers.add(
                allOf(
                    containsString("exists but is not supported when running in serverless mode"),
                    containsString(ServerlessCustomRoleValidator.mustBePredefinedClusterPrivilegeMessage())
                )
            );
        }
        if (unknownIndexPrivilege) {
            itemMatchers.add(
                allOf(
                    containsString("unknown index privilege"),
                    containsString(ServerlessCustomRoleValidator.mustBePredefinedIndexPrivilegeMessage())
                )
            );
        }
        if (unsupportedIndexPrivilege) {
            itemMatchers.add(
                allOf(
                    containsString("exists but is not supported when running in serverless mode"),
                    containsString(ServerlessCustomRoleValidator.mustBePredefinedIndexPrivilegeMessage())
                )
            );
        }
        if (restrictedIndexAccess) {
            itemMatchers.add(containsString("access to restricted indices is not supported when running in serverless mode"));
        }
        if (invalidApplicationName) {
            itemMatchers.add(containsString("invalid application name"));
        }
        if (invalidApplicationPrivilege) {
            itemMatchers.add(containsString("Application privilege names and actions must match the pattern"));
        }
        assertThat(validationErrors, containsInAnyOrder(itemMatchers));
    }

    public static RoleDescriptor randomRoleDescriptor() {
        return randomRoleDescriptor(true);
    }

    public static RoleDescriptor randomRoleDescriptorWithoutFlsDls() {
        return randomRoleDescriptor(false);
    }

    private static RoleDescriptor randomRoleDescriptor(boolean allowDlsFls) {
        return new RoleDescriptor(
            randomValueOtherThanMany(ReservedRolesStore::isReserved, () -> randomAlphaOfLengthBetween(3, 90)),
            randomSubsetOf(ServerlessSupportedPrivilegesRegistry.supportedClusterPrivilegeNames()).toArray(String[]::new),
            randomIndicesPrivileges(0, 3, Set.of(), allowDlsFls),
            randomApplicationPrivileges(),
            null,
            null,
            Map.of(),
            Map.of(),
            null,
            null
        );
    }

    private static RoleDescriptor.IndicesPrivileges[] randomIndicesPrivileges(
        int min,
        int max,
        Set<String> excludedPrivileges,
        boolean allowDlsFls
    ) {
        final RoleDescriptor.IndicesPrivileges[] indexPrivileges = new RoleDescriptor.IndicesPrivileges[randomIntBetween(min, max)];
        for (int i = 0; i < indexPrivileges.length; i++) {
            indexPrivileges[i] = randomIndicesPrivilegesBuilder(excludedPrivileges, allowDlsFls).build();
        }
        return indexPrivileges;
    }

    private static RoleDescriptor.IndicesPrivileges.Builder randomIndicesPrivilegesBuilder(
        Set<String> excludedPrivileges,
        boolean allowDlsFls
    ) {
        final Set<String> candidatePrivilegesNames = Sets.difference(
            ServerlessSupportedPrivilegesRegistry.supportedIndexPrivilegeNames(),
            excludedPrivileges
        );
        assert false == candidatePrivilegesNames.isEmpty() : "no candidate privilege names to random from";
        return indexBuilderWithPrivileges(randomSubsetOf(randomIntBetween(1, 4), candidatePrivilegesNames), false, allowDlsFls);
    }

    public static RoleDescriptor.IndicesPrivileges.Builder indexBuilderWithPrivileges(
        List<String> privileges,
        boolean allowRestrictedIndices,
        boolean allowDlsFls
    ) {
        final RoleDescriptor.IndicesPrivileges.Builder builder = RoleDescriptor.IndicesPrivileges.builder()
            .privileges(privileges)
            .indices(generateRandomStringArray(5, randomIntBetween(3, 9), false, false))
            .allowRestrictedIndices(allowRestrictedIndices);
        if (allowDlsFls) {
            randomDlsFls(builder);
        }
        return builder;
    }

    private static void randomDlsFls(RoleDescriptor.IndicesPrivileges.Builder builder) {
        if (randomBoolean()) {
            builder.query(
                randomBoolean()
                    ? "{ \"term\": { \"" + randomAlphaOfLengthBetween(3, 24) + "\" : \"" + randomAlphaOfLengthBetween(3, 24) + "\" }"
                    : "{ \"match_all\": {} }"
            );
        }
        if (randomBoolean()) {
            if (randomBoolean()) {
                builder.grantedFields("*");
                builder.deniedFields(generateRandomStringArray(4, randomIntBetween(4, 9), false, false));
            } else {
                builder.grantedFields(generateRandomStringArray(4, randomIntBetween(4, 9), false, false));
            }
        }
    }

    private static RoleDescriptor.ApplicationResourcePrivileges[] randomApplicationPrivileges() {
        final RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges =
            new RoleDescriptor.ApplicationResourcePrivileges[randomIntBetween(0, 2)];
        for (int i = 0; i < applicationPrivileges.length; i++) {
            final RoleDescriptor.ApplicationResourcePrivileges.Builder builder = RoleDescriptor.ApplicationResourcePrivileges.builder();
            builder.application(randomBoolean() ? "*" : randomFrom(SUPPORTED_APPLICATION_NAMES.toArray(new String[0])));
            if (randomBoolean()) {
                builder.privileges(randomNonEmptySubsetOf(List.of("*", "action:read", "action:*", "action/read:data")));
            } else {
                builder.privileges(generateRandomStringArray(6, randomIntBetween(4, 8), false, false));
            }
            if (randomBoolean()) {
                builder.resources("*");
            } else {
                builder.resources(generateRandomStringArray(6, randomIntBetween(4, 8), false, false));
            }
            applicationPrivileges[i] = builder.build();
        }
        return applicationPrivileges;
    }
}
