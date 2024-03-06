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

import co.elastic.elasticsearch.serverless.security.role.ServerlessCustomRoleValidator;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.PATH_RESTRICTED;
import static org.hamcrest.core.IsEqual.equalTo;

public class ServerlessUpdateApiKeyRequestTranslatorTests extends ESTestCase {

    public void testValidPayload() throws IOException {
        var strictValidationEnabled = randomBoolean();
        var translator = new ServerlessUpdateApiKeyRequestTranslator(new ServerlessCustomRoleValidator(), () -> strictValidationEnabled);

        final String json = "{ \"role_descriptors\": { \"role-a\": {\"cluster\":[\"all\"]} } }";
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(json),
            XContentType.JSON
        ).withParams(Map.of("ids", "id", PATH_RESTRICTED, String.valueOf(randomBoolean()))).build();
        var actual = translator.translate(restRequest);
        assertThat(actual.getRoleDescriptors().size(), equalTo(1));
        assertThat(
            actual.getRoleDescriptors().get(0),
            equalTo(new RoleDescriptor("role-a", new String[] { "all" }, null, null, null, null, null, null))
        );
    }

    public void testStrictValidationDisabled() throws IOException {
        var translator = new ServerlessUpdateApiKeyRequestTranslator(new ServerlessCustomRoleValidator(), () -> false);

        final String json = "{ \"role_descriptors\": { \"role-a\": {\"cluster\":[\"manage_ilm\"]} } }";
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(json),
            XContentType.JSON
        ).withParams(Map.of("ids", "id", PATH_RESTRICTED, String.valueOf(randomBoolean()))).build();
        var actual = translator.translate(restRequest);
        assertThat(actual.getRoleDescriptors().size(), equalTo(1));
        assertThat(
            actual.getRoleDescriptors().get(0),
            equalTo(new RoleDescriptor("role-a", new String[] { "manage_ilm" }, null, null, null, null, null, null))
        );
    }

    public void testStrictValidationEnabled() {
        var translator = new ServerlessUpdateApiKeyRequestTranslator(new ServerlessCustomRoleValidator(), () -> true);

        final String json = "{ \"role_descriptors\": { \"role-a\": {\"cluster\":[\"manage_ilm\"]} } }";
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(json),
            XContentType.JSON
        ).withParams(Map.of("ids", "id", PATH_RESTRICTED, String.valueOf(true))).build();
        expectThrows(ActionRequestValidationException.class, () -> translator.translate(restRequest));
    }
}
