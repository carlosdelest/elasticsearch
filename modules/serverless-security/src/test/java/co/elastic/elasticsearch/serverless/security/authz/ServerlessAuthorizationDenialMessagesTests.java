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

package co.elastic.elasticsearch.serverless.security.authz;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.ilm.action.GetLifecycleAction;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationDenialMessages;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

public class ServerlessAuthorizationDenialMessagesTests extends ESTestCase {
    private final ServerlessAuthorizationDenialMessages denialMessages = new ServerlessAuthorizationDenialMessages();

    public void testClusterActionDeniedMessageOnlyIncludesSupportedPrivileges() {
        final User user = new User(randomAlphaOfLengthBetween(6, 8), randomAlphaOfLengthBetween(6, 8));
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new Authentication.RealmRef("test", "test", "foo"))
            .build(false);

        final TransportRequest request = new GetLifecycleAction.Request(randomAlphaOfLengthBetween(6, 8));

        // Note: in practice this should never happen since ILM is unavailable, but it's a valid test to exercise the overall flow
        final String actionName = GetLifecycleAction.NAME;
        final String actualServerless = denialMessages.actionDenied(authentication, null, actionName, request, null);
        assertThat(actualServerless, containsString("[" + actionName + "] is unauthorized for user [" + user.principal() + "]"));
        assertThat(actualServerless, containsString("this action is granted by the cluster privileges [manage,all]"));

        final String actualStateful = new AuthorizationDenialMessages.Default().actionDenied(
            authentication,
            null,
            actionName,
            request,
            null
        );
        assertThat(actualStateful, containsString("[" + actionName + "] is unauthorized for user [" + user.principal() + "]"));
        assertThat(actualStateful, containsString("this action is granted by the cluster privileges [read_ilm,manage_ilm,manage,all]"));
    }

    public void testIndexActionDeniedMessageOnlyIncludesSupportedPrivileges() {
        final User user = new User(randomAlphaOfLengthBetween(6, 8), randomAlphaOfLengthBetween(6, 8));
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(user)
            .realmRef(new Authentication.RealmRef("test", "test", "foo"))
            .build(false);

        final TransportRequest request = mock(TransportRequest.class);

        final String actionName = ExplainLifecycleAction.NAME;
        final String actualServerless = denialMessages.actionDenied(authentication, null, actionName, request, null);
        assertThat(actualServerless, containsString("[" + actionName + "] is unauthorized for user [" + user.principal() + "]"));
        assertThat(actualServerless, containsString("this action is granted by the index privileges [view_index_metadata,manage,all]"));

        final String actualStateful = new AuthorizationDenialMessages.Default().actionDenied(
            authentication,
            null,
            actionName,
            request,
            null
        );
        assertThat(actualStateful, containsString("[" + actionName + "] is unauthorized for user [" + user.principal() + "]"));
        assertThat(
            actualStateful,
            containsString("this action is granted by the index privileges [manage_ilm,view_index_metadata,manage,all]")
        );
    }

}
