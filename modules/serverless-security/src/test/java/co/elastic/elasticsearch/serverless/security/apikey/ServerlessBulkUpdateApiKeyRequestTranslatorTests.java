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

package co.elastic.elasticsearch.serverless.security.apikey;

import co.elastic.elasticsearch.serverless.security.role.ServerlessRoleValidator;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.core.IsEqual.equalTo;

public class ServerlessBulkUpdateApiKeyRequestTranslatorTests extends ESTestCase {

    public void testValidPayload() throws IOException {
        var operatorStrictRoleValidationEnabled = randomBoolean();
        var translator = new ServerlessBulkUpdateApiKeyRequestTranslator(
            new ServerlessRoleValidator(),
            () -> operatorStrictRoleValidationEnabled
        );

        final String json = "{ \"ids\": [\"id\"], \"role_descriptors\": { \"role-a\": {\"cluster\":[\"all\"]} } }";
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(json),
            XContentType.JSON
        ).withParams(withRandomlyIncludedServerlessNonOperatorRequestParam()).build();
        var actual = translator.translate(restRequest);
        assertThat(actual.getRoleDescriptors().size(), equalTo(1));
        assertThat(
            actual.getRoleDescriptors().get(0),
            equalTo(new RoleDescriptor("role-a", new String[] { "all" }, null, null, null, null, null, null))
        );
    }

    public void testStrictOperatorRoleValidationDisabled() throws IOException {
        var translator = new ServerlessBulkUpdateApiKeyRequestTranslator(new ServerlessRoleValidator(), () -> false);

        final String json = "{ \"ids\": [\"id\"], \"role_descriptors\": { \"role-a\": {\"cluster\":[\"manage_ilm\"]} } }";
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(json),
            XContentType.JSON
        ).build();
        var actual = translator.translate(restRequest);
        assertThat(actual.getRoleDescriptors().size(), equalTo(1));
        assertThat(
            actual.getRoleDescriptors().get(0),
            equalTo(new RoleDescriptor("role-a", new String[] { "manage_ilm" }, null, null, null, null, null, null))
        );
    }

    public void testStrictValidationEnabled() {
        var translator = new ServerlessBulkUpdateApiKeyRequestTranslator(new ServerlessRoleValidator(), () -> true);

        final String json = "{ \"ids\": [\"id\"], \"role_descriptors\": { \"role-a\": {\"cluster\":[\"manage_ilm\"]} } }";
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(json),
            XContentType.JSON
        ).withParams(withRandomlyIncludedServerlessNonOperatorRequestParam()).build();
        expectThrows(ActionRequestValidationException.class, () -> translator.translate(restRequest));
    }

    private static Map<String, String> withRandomlyIncludedServerlessNonOperatorRequestParam() {
        return ServerlessUpdateApiKeyRequestTranslatorTests.withRandomlyIncludedServerlessNonOperatorRequestParam(Map.of());
    }
}
