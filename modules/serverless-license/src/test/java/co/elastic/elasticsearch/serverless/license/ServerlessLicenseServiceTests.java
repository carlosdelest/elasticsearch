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
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalToIgnoringCase;

public class ServerlessLicenseServiceTests extends ESTestCase {

    private ServerlessLicenseService serverlessLicenseService;

    @Before
    public void setup() {
        serverlessLicenseService = new ServerlessLicenseService();
    }

    public void testGetLicense() {
        License license = serverlessLicenseService.getLicense();
        assertThat(license.type(), equalToIgnoringCase(License.LicenseType.ENTERPRISE.toString()));
        assertThat(license.issueDate(), is(0L));
        assertThat(license.startDate(), is(0L));
        assertThat(license.expiryDate(), is(4102444800000L));
        assertThat(license.maxResourceUnits(), is(100_000));
        assertThat(license.issuer(), is("Elastic"));
        assertThat(license.issuedTo(), is("Elastic Serverless"));
        assertThat(license.uid(), is(UUID.nameUUIDFromBytes("You Know, for Search".getBytes(StandardCharsets.UTF_8)).toString()));
    }

    public void testRegisterLicense() {
        expectThrows(UnsupportedOperationException.class, () -> serverlessLicenseService.registerLicense(null, null));
    }

    public void testRemoveLicense() {
        expectThrows(UnsupportedOperationException.class, () -> serverlessLicenseService.removeLicense(null));
    }

    public void testStartBasicLicense() {
        expectThrows(UnsupportedOperationException.class, () -> serverlessLicenseService.startBasicLicense(null, null));
    }

    public void testStartTrialLicense() {
        expectThrows(UnsupportedOperationException.class, () -> serverlessLicenseService.startTrialLicense(null, null));
    }
}
