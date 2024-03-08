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

package co.elastic.elasticsearch.serverless.security.privilege;

import co.elastic.elasticsearch.serverless.security.role.ServerlessCustomRoleValidator;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static co.elastic.elasticsearch.serverless.security.role.ServerlessCustomRoleValidatorTests.indexBuilderWithPrivileges;
import static co.elastic.elasticsearch.serverless.security.role.ServerlessCustomRoleValidatorTests.randomRoleDescriptorWithoutFlsDls;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.mockito.Mockito.mock;

public class ServerlessHasPrivilegesRequestBuilderTests extends ESTestCase {

    @BeforeClass
    public static void init() {
        // necessary since the validator calls ReservedRolesStore::isReserved under the hood which is not available unless the class is
        // initialized
        new ReservedRolesStore();
    }

    public void testValidRequest() throws IOException {
        final Map<String, Object> privilegesToCheck = randomValidPrivilegesToCheckAsMap();
        final ServerlessHasPrivilegesRequestBuilderFactory.ServerlessHasPrivilegesRequestBuilder builder =
            new ServerlessHasPrivilegesRequestBuilderFactory.ServerlessHasPrivilegesRequestBuilder(
                mock(),
                randomBoolean(),
                ESTestCase::randomBoolean
            );
        final HasPrivilegesRequestBuilder actual = builder.source("username", mapToBytes(privilegesToCheck), XContentType.JSON);
        assertThat(actual.request(), is(notNullValue()));
    }

    public void testValidRequestWithRawActions() throws IOException {
        final RoleDescriptor roleDescriptor = new RoleDescriptor(
            randomAlphaOfLength(20),
            new String[] { "cluster:" + randomFrom(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10) + "/*") },
            new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                    .indices(generateRandomStringArray(10, 10, false, false))
                    .privileges("indices:" + randomFrom(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10) + "/*"))
                    .build() },
            null
        );
        final ServerlessHasPrivilegesRequestBuilderFactory.ServerlessHasPrivilegesRequestBuilder builder =
            new ServerlessHasPrivilegesRequestBuilderFactory.ServerlessHasPrivilegesRequestBuilder(
                mock(),
                randomBoolean(),
                ESTestCase::randomBoolean
            );
        final HasPrivilegesRequestBuilder actual = builder.source(
            roleDescriptor.getName(),
            mapToBytes(roleDescriptorToPrivilegesToCheckMap(roleDescriptor)),
            XContentType.JSON
        );
        assertThat(actual.request(), is(notNullValue()));
    }

    public void testInvalidRequest() throws IOException {
        final boolean unknownClusterPrivilege = randomBoolean();
        final boolean unsupportedClusterPrivilege = randomBoolean();
        final boolean unknownIndexPrivilege = randomBoolean();
        // ensure at least one validation error
        final boolean unsupportedIndexPrivilege = false == (unknownClusterPrivilege && unsupportedClusterPrivilege && unknownIndexPrivilege)
            || randomBoolean();

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
        final RoleDescriptor.IndicesPrivileges[] indicesPrivileges = new RoleDescriptor.IndicesPrivileges[] {
            indexBuilderWithPrivileges(indexPrivileges, randomBoolean(), false).build() };

        final RoleDescriptor roleDescriptor = new RoleDescriptor(
            randomAlphaOfLength(10),
            clusterPrivileges.toArray(String[]::new),
            indicesPrivileges,
            RoleDescriptorTests.randomApplicationPrivileges(),
            null,
            null,
            Map.of(),
            Map.of(),
            null,
            null
        );

        final Map<String, Object> privilegesToCheck = roleDescriptorToPrivilegesToCheckMap(roleDescriptor);
        final ServerlessHasPrivilegesRequestBuilderFactory.ServerlessHasPrivilegesRequestBuilder builder =
            new ServerlessHasPrivilegesRequestBuilderFactory.ServerlessHasPrivilegesRequestBuilder(mock(), true, () -> true);

        final ActionRequestValidationException ex = expectThrows(
            ActionRequestValidationException.class,
            () -> builder.source(roleDescriptor.getName(), mapToBytes(privilegesToCheck), XContentType.JSON)
        );
        assertThat(ex, Matchers.is(Matchers.notNullValue()));
        final List<String> validationErrors = ex.validationErrors();
        final List<Matcher<? super String>> itemMatchers = new ArrayList<>();
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
        assertThat(validationErrors, containsInAnyOrder(itemMatchers));
    }

    public void testUseOfFieldLevelSecurityThrowsException() {
        final var json = """
            {
              "index": [
                {
                  "names": [ "employees" ],
                  "privileges": [ "read", "write" ],
                  "field_security": {
                    "grant": [ "name", "department", "title" ]
                  }
                }
              ]
            }""";

        final HasPrivilegesRequestBuilder builder = new HasPrivilegesRequestBuilder(mock(Client.class));
        final ElasticsearchParseException parseException = expectThrows(
            ElasticsearchParseException.class,
            () -> builder.source("elastic", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON)
        );
        assertThat(parseException.getMessage(), containsString("[field_security]"));
    }

    private HashMap<String, Object> randomValidPrivilegesToCheckAsMap() throws IOException {
        return roleDescriptorToPrivilegesToCheckMap(randomRoleDescriptorWithoutFlsDls());
    }

    private HashMap<String, Object> roleDescriptorToPrivilegesToCheckMap(RoleDescriptor roleDescriptor) throws IOException {
        final var map = new HashMap<>(
            convertToMap(
                JsonXContent.jsonXContent,
                Strings.toString(roleDescriptor.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS)),
                false
            )
        );
        // Need to rename and strip some fields because RoleDescriptor and PrivilegesToCheck insist of different default field names
        if (map.containsKey("indices")) {
            map.put("index", map.get("indices"));
            map.remove("indices");
        }
        if (map.containsKey("applications")) {
            map.put("application", map.get("applications"));
            map.remove("applications");
        }
        map.remove("run_as");
        map.remove("metadata");
        map.remove("transient_metadata");
        return map;
    }

    private static BytesReference mapToBytes(Map<String, ?> map) throws IOException {
        return BytesReference.bytes(XContentFactory.jsonBuilder().map(map));
    }
}
