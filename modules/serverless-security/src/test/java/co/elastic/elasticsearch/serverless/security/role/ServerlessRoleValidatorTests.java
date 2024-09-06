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
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleRestrictionTests;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static co.elastic.elasticsearch.serverless.security.role.ServerlessRoleValidator.PREDEFINED_ROLE_METADATA_ALLOWLIST;
import static co.elastic.elasticsearch.serverless.security.role.ServerlessRoleValidator.PUBLIC_METADATA_KEY;
import static co.elastic.elasticsearch.serverless.security.role.ServerlessRoleValidator.RESERVED_ROLE_NAME_PREFIX;
import static co.elastic.elasticsearch.serverless.security.role.ServerlessRoleValidator.SUPPORTED_APPLICATION_NAMES;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiAlphanumOfLengthBetween;
import static org.elasticsearch.xpack.core.security.support.Validation.Roles.MAX_DESCRIPTION_LENGTH;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ServerlessRoleValidatorTests extends ESTestCase {

    @BeforeClass
    public static void init() {
        // necessary since the validator calls ReservedRolesStore::isReserved under the hood which is not available unless the class is
        // initialized
        new ReservedRolesStore();
    }

    public void testValidCustomRole() {
        final ServerlessRoleValidator validator = new ServerlessRoleValidator();
        assertThat(validator.validateCustomRole(randomRoleDescriptor()), is(nullValue()));
    }

    public void testValidPublicPredefinedRole() {
        final ServerlessRoleValidator validator = new ServerlessRoleValidator();
        var role = randomRoleDescriptor(true, false, true, Map.of(PUBLIC_METADATA_KEY, true));
        assertThat(validator.validatePredefinedRole(role), is(nullValue()));
    }

    public void testValidCustomRoleCannotHavePublicMetadataFlag() {
        final ServerlessRoleValidator validator = new ServerlessRoleValidator();
        var role = randomRoleDescriptor(true, false, true, Map.of(PUBLIC_METADATA_KEY, true));
        var ex = validator.validateCustomRole(role);
        assertThat(ex.getMessage(), containsString("role descriptor metadata keys may not start with [_] but found these keys: [_public]"));
    }

    public void testNonpublicPredefinedRoleNotLimited() {
        final ServerlessRoleValidator validator = new ServerlessRoleValidator();
        final List<String> indexPrivileges = new ArrayList<>();
        indexPrivileges.add(
            randomValueOtherThanMany(
                ServerlessSupportedPrivilegesRegistry.supportedIndexPrivilegeNames()::contains,
                () -> randomFrom(IndexPrivilege.names().toArray(new String[0]))
            )
        );
        indexPrivileges.add(randomFrom(ServerlessSupportedPrivilegesRegistry.supportedIndexPrivilegeNames().toArray(new String[0])));
        final RoleDescriptor role = new RoleDescriptor(
            RESERVED_ROLE_NAME_PREFIX + randomAlphaOfLengthBetween(5, 10),
            null,
            new RoleDescriptor.IndicesPrivileges[] { indexBuilderWithPrivileges(indexPrivileges, true, true).build() },
            null,
            null,
            null,
            Map.of(),
            Map.of(),
            null,
            null,
            null,
            null
        );
        assertThat(validator.validatePredefinedRole(role), is(nullValue()));
    }

    public void testPublicPredefinedRoleDisallowsOtherReservedMetadata() {
        final ServerlessRoleValidator validator = new ServerlessRoleValidator();
        String unknownMetadataKey = randomValueOtherThanMany(
            PREDEFINED_ROLE_METADATA_ALLOWLIST::contains,
            () -> MetadataUtils.RESERVED_PREFIX + randomAlphaOfLengthBetween(2, 10)
        );
        var role = randomRoleDescriptor(true, false, true, Map.of(PUBLIC_METADATA_KEY, true, unknownMetadataKey, randomInt()));
        assertThat(
            validator.validatePredefinedRole(role).getMessage(),
            containsString(
                Strings.format("role descriptor metadata keys may not start with [_] but found these keys: [%s]", unknownMetadataKey)
            )
        );
    }

    public void testPrivatePredefinedRoleAllowsOtherReservedMetadata() {
        final ServerlessRoleValidator validator = new ServerlessRoleValidator();
        String unknownMetadataKey = randomValueOtherThanMany(
            PREDEFINED_ROLE_METADATA_ALLOWLIST::contains,
            () -> MetadataUtils.RESERVED_PREFIX + randomAlphaOfLengthBetween(2, 10)
        );
        var role = randomRoleDescriptor(true, false, true, Map.of(PUBLIC_METADATA_KEY, false, unknownMetadataKey, randomInt()));
        assertThat(validator.validatePredefinedRole(role), is(nullValue()));
    }

    public void testInvalidCustomRoleName() {
        final ServerlessRoleValidator validator = new ServerlessRoleValidator();

        Map.ofEntries(
            Map.entry("superuser", containsString("is reserved and may not be used")),
            Map.entry("", containsString("role name must be at least 1 and no more than 507 characters")),
            Map.entry(randomAlphaOfLength(508), containsString("role name must be at least 1 and no more than 507 characters")),
            Map.entry(
                randomFrom("_", ".", "-", " ", "*", "#") + randomAlphaOfLength(30),
                containsString("role name must begin with a letter or digit")
            ),
            Map.entry("this role name has spaces", containsString("can only contain letters, digits and the characters '_', '-', and '.'")),
            Map.entry(
                "111sdcd32<bajadsklasdsadasda",
                containsString("can only contain letters, digits and the characters '_', '-', and '.'")
            ),
            Map.entry("*%&*#^()@^#(*)<>|![]`~", containsString("can only contain letters, digits and the characters '_', '-', and '.'"))
        ).entrySet().forEach(testCase -> {
            final String roleName = testCase.getKey();
            final Matcher<String> validationErrorMatcher = testCase.getValue();

            final ActionRequestValidationException ex = validator.validateCustomRole(
                new RoleDescriptor(roleName, null, null, null, null, null, Map.of(), Map.of(), null, null, null, null)
            );
            assertThat("validating role name: " + roleName, ex, is(notNullValue()));
            assertThat(ex.validationErrors(), containsInAnyOrder(validationErrorMatcher));
        });

    }

    public void testValidCustomRoleName() {
        final ServerlessRoleValidator validator = new ServerlessRoleValidator();
        Set.of(
            "my-role.123_45",
            randomAlphaOfLength(1),
            "123846198498317",
            UUID.randomUUID().toString(),
            randomAsciiAlphanumOfLengthBetween(1, 5) + randomFrom("_", ".", "-") + randomAsciiAlphanumOfLengthBetween(5, 10),
            String.valueOf(System.currentTimeMillis()),
            randomFrom("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
        ).forEach(roleName -> {
            final ActionRequestValidationException ex = validator.validateCustomRole(
                new RoleDescriptor(roleName, null, null, null, null, null, Map.of(), Map.of(), null, null, null, null)
            );
            assertThat("validating role name: " + roleName, ex, is(nullValue()));
        });

    }

    public void testValidReservedRoleName() {
        Set.of(
            "superuser",
            "viewer",
            "remote_monitoring_agent",
            "t1_analyst",
            "threat_intelligence_analyst",
            "_elastic_pod_autoscaler",
            "12846123796498123",
            "_my-reserved-role.123",
            randomFrom("_", ".", "-") + randomAsciiAlphanumOfLengthBetween(5, 10),
            randomAsciiAlphanumOfLengthBetween(1, 20),
            String.valueOf(System.currentTimeMillis()),
            randomAlphaOfLength(1),
            randomFrom("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
        ).forEach(roleName -> {
            var validationError = ServerlessRoleValidator.validateRoleName(roleName, true);
            assertThat("while validating role: " + roleName, validationError, is(nullValue()));
        });
    }

    public void testInvalidReservedRoleName() {
        Map.ofEntries(
            Map.entry("", containsString("role name must be at least 1 and no more than 507 characters")),
            Map.entry(randomAlphaOfLength(508), containsString("role name must be at least 1 and no more than 507 characters")),
            Map.entry("this role name has spaces", containsString("can only contain letters, digits and the characters '_', '-', and '.'")),
            Map.entry(
                "111sdcd32~bajadsklasdsadasda",
                containsString("can only contain letters, digits and the characters '_', '-', and '.'")
            ),
            Map.entry("*%&*#^()@^#(*)<>|![]`~", containsString("can only contain letters, digits and the characters '_', '-', and '.'"))
        ).entrySet().forEach(testCase -> {
            final String roleName = testCase.getKey();
            final Matcher<String> validationErrorMatcher = testCase.getValue();

            var validationError = ServerlessRoleValidator.validateRoleName(roleName, true);
            assertThat(validationError, is(notNullValue()));
            assertThat("validating role name: " + roleName, validationError, validationErrorMatcher);
        });

    }

    public void testInvalidCustomRole() {
        final ServerlessRoleValidator validator = new ServerlessRoleValidator();

        final int roleNameCaseNo = randomIntBetween(0, 3);
        final String roleName = switch (roleNameCaseNo) {
            case 0 -> randomAlphaOfLength(30); // valid
            case 1 -> "superuser"; // reserved
            case 2 -> "_" + randomAlphaOfLength(30); // invalid prefix
            case 3 -> "this role name has spaces"; // spaces are not allowed
            default -> throw new IllegalStateException("Unexpected value: " + roleNameCaseNo);
        };
        final boolean invalidRoleName = roleNameCaseNo != 0;
        final boolean unknownClusterPrivilege = randomBoolean();
        final boolean unsupportedClusterPrivilege = randomBoolean();
        final boolean unknownIndexPrivilege = randomBoolean();
        final boolean unsupportedIndexPrivilege = randomBoolean();
        final boolean restrictedIndexAccess = randomBoolean();
        final boolean invalidApplicationName = randomBoolean();
        final boolean invalidApplicationPrivilege = randomBoolean();
        final boolean invalidDescription = randomBoolean();
        // ensure at least one validation error
        final boolean invalidWorkflowRestriction = false == (invalidRoleName
            && unknownClusterPrivilege
            && unsupportedClusterPrivilege
            && unknownIndexPrivilege
            && unsupportedIndexPrivilege
            && restrictedIndexAccess
            && invalidApplicationName
            && invalidApplicationPrivilege
            && invalidDescription) || randomBoolean();

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

        final RoleDescriptor.Restriction restriction;
        if (invalidWorkflowRestriction) {
            restriction = new RoleDescriptor.Restriction(new String[] { randomAlphaOfLength(4) });
        } else {
            restriction = RoleRestrictionTests.randomWorkflowsRestriction(1, 2);
        }
        final String description;
        if (invalidDescription) {
            description = randomAlphaOfLength(MAX_DESCRIPTION_LENGTH + 1);
        } else {
            description = randomAlphaOfLength(20);
        }

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
            null,
            restriction,
            description
        );

        final ActionRequestValidationException ex = validator.validateCustomRole(roleDescriptor);
        assertThat(ex, is(notNullValue()));
        final List<String> validationErrors = ex.validationErrors();
        final List<Matcher<? super String>> itemMatchers = new ArrayList<>();
        switch (roleNameCaseNo) {
            case 1 -> {
                itemMatchers.add(containsString("is reserved and may not be used"));
            }
            case 2, 3 -> itemMatchers.add(containsString("can only contain letters, digits and the characters '_', '-', and '.'"));
        }
        if (unknownClusterPrivilege) {
            itemMatchers.add(
                allOf(
                    containsString("unknown cluster privilege"),
                    containsString(ServerlessRoleValidator.mustBePredefinedClusterPrivilegeMessage())
                )
            );
        }
        if (unsupportedClusterPrivilege) {
            itemMatchers.add(
                allOf(
                    containsString("exists but is not supported when running in serverless mode"),
                    containsString(ServerlessRoleValidator.mustBePredefinedClusterPrivilegeMessage())
                )
            );
        }
        if (unknownIndexPrivilege) {
            itemMatchers.add(
                allOf(
                    containsString("unknown index privilege"),
                    containsString(ServerlessRoleValidator.mustBePredefinedIndexPrivilegeMessage())
                )
            );
        }
        if (unsupportedIndexPrivilege) {
            itemMatchers.add(
                allOf(
                    containsString("exists but is not supported when running in serverless mode"),
                    containsString(ServerlessRoleValidator.mustBePredefinedIndexPrivilegeMessage())
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
        if (invalidWorkflowRestriction) {
            itemMatchers.add(containsString("Unknown workflow"));
        }
        if (invalidDescription) {
            itemMatchers.add(containsString(Strings.format("Role description must be less than %s characters.", MAX_DESCRIPTION_LENGTH)));
        }
        assertThat(validationErrors, containsInAnyOrder(itemMatchers));
    }

    public static RoleDescriptor randomRoleDescriptor() {
        return randomRoleDescriptor(true, true, true, Map.of());
    }

    public static RoleDescriptor randomRoleDescriptorWithoutFlsDlsOrRestriction() {
        return randomRoleDescriptor(false, false, false, Map.of());
    }

    private static RoleDescriptor randomRoleDescriptor(
        boolean allowDlsFls,
        boolean allowRestriction,
        boolean allowDescription,
        Map<String, Object> metadata
    ) {
        return new RoleDescriptor(
            randomValueOtherThanMany(ReservedRolesStore::isReserved, () -> randomAlphaOfLengthBetween(3, 90)),
            randomSubsetOf(ServerlessSupportedPrivilegesRegistry.supportedClusterPrivilegeNames()).toArray(String[]::new),
            randomIndicesPrivileges(0, 3, Set.of(), allowDlsFls),
            randomApplicationPrivileges(),
            null,
            null,
            metadata,
            Map.of(),
            null,
            null,
            allowRestriction ? RoleRestrictionTests.randomWorkflowsRestriction(1, 2) : null,
            allowDescription ? randomAlphaOfLengthBetween(0, 20) : null
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
