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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;

public class ServerlessCreateApiKeyRequestBuilderTests extends ESTestCase {

    public void testValidPayload() throws IOException {
        var strictValidationEnabled = randomBoolean();

        var builder = new ServerlessCreateApiKeyRequestBuilderFactory.ServerlessCreateApiKeyRequestBuilder(
            mock(Client.class),
            randomBoolean(),
            () -> strictValidationEnabled
        );

        final String json = "{ \"name\": \"name\", \"role_descriptors\": { \"role-a\": {\"cluster\":[\"all\"]} } }";
        var actual = builder.parse(new BytesArray(json), XContentType.JSON);
        assertThat(actual.getRoleDescriptors().size(), equalTo(1));
        assertThat(
            actual.getRoleDescriptors().get(0),
            equalTo(new RoleDescriptor("role-a", new String[] { "all" }, null, null, null, null, null, null))
        );
    }

    public void testStrictValidationDisabled() throws IOException {
        var builder = new ServerlessCreateApiKeyRequestBuilderFactory.ServerlessCreateApiKeyRequestBuilder(
            mock(Client.class),
            randomBoolean(),
            () -> false
        );

        final String json = "{ \"name\": \"name\", \"role_descriptors\": { \"role-a\": {\"cluster\":[\"manage_ilm\"]} } }";
        var actual = builder.parse(new BytesArray(json), XContentType.JSON);
        assertThat(actual.getRoleDescriptors().size(), equalTo(1));
        assertThat(
            actual.getRoleDescriptors().get(0),
            equalTo(new RoleDescriptor("role-a", new String[] { "manage_ilm" }, null, null, null, null, null, null))
        );
    }

    public void testStrictValidationEnabled() {
        var builder = new ServerlessCreateApiKeyRequestBuilderFactory.ServerlessCreateApiKeyRequestBuilder(
            mock(Client.class),
            true,
            () -> true
        );

        final String json = "{ \"name\": \"name\", \"role_descriptors\": { \"role-a\": {\"cluster\":[\"manage_ilm\"]} } }";
        expectThrows(ActionRequestValidationException.class, () -> builder.parse(new BytesArray(json), XContentType.JSON));
    }
}
