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

package co.elastic.elasticsearch.serverless.license;

import org.elasticsearch.license.License;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collection;
import java.util.Collections;

public class ServerlessLicensePlugin extends Plugin {
    @Override
    public Collection<?> createComponents(PluginServices services) {
        if (ServerlessLicenseService.UUID.equals(XPackPlugin.getSharedLicenseService().getLicense().uid()) == false
            || License.OperationMode.ENTERPRISE.equals(XPackPlugin.getSharedLicenseState().getOperationMode()) == false) {
            // The serverless license is loaded via SPI in the XPack core module.
            // This error should never happen and is only a sanity check.
            throw new AssertionError("The serverless license is not as expected.");
        }
        return Collections.emptyList();

    }
}
