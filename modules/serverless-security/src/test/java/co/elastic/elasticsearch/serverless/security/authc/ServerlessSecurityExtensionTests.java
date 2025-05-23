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

package co.elastic.elasticsearch.serverless.security.authc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.SecurityExtension;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServerlessSecurityExtensionTests extends ESTestCase {

    public void testMultiplePerProjectFileRealmsFails() {
        Settings.Builder builder = Settings.builder()
            .put("xpack.security.authc.realms.project_file_settings.f1.order", 10)
            .put("xpack.security.authc.realms.project_file_settings.f2.order", 11);

        var securityComponent = mock(SecurityExtension.SecurityComponents.class);
        when(securityComponent.settings()).thenReturn(builder.build());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ServerlessSecurityExtension().getRealms(securityComponent)
        );
        assertThat(
            e.getMessage(),
            containsString(
                "Multiple [project_file_settings] realms are configured: [project_file_settings/f1, project_file_settings/f2]. "
                    + "Only one such realm can be configured."
            )
        );
    }
}
