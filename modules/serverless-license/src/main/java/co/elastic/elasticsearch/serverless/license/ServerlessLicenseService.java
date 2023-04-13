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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.license.License;
import org.elasticsearch.license.PostStartBasicRequest;
import org.elasticsearch.license.PostStartBasicResponse;
import org.elasticsearch.license.PostStartTrialRequest;
import org.elasticsearch.license.PostStartTrialResponse;
import org.elasticsearch.license.PutLicenseRequest;
import org.elasticsearch.license.internal.MutableLicenseService;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;

import java.io.IOException;
import java.time.Instant;

public class ServerlessLicenseService extends AbstractLifecycleComponent implements MutableLicenseService {

    static final String UUID = "d85d2c6a-b96d-3cc6-96db-5571a789b156"; // You Know, for Search

    @Override
    public License getLicense() {
        return new License.Builder().type(License.LicenseType.ENTERPRISE)
            .issueDate(Instant.EPOCH.toEpochMilli())
            .startDate(Instant.EPOCH.toEpochMilli())
            .expiryDate(Instant.parse("2100-01-01T00:00:00Z").toEpochMilli())
            .maxResourceUnits(100_000)
            .issuedTo("Elastic Serverless")
            .issuer("Elastic")
            .uid(UUID)
            .build();
    }

    @Override
    public void registerLicense(PutLicenseRequest request, ActionListener<PutLicenseResponse> listener) {
        throw new UnsupportedOperationException("registering a license in serverless is not supported");
    }

    @Override
    public void removeLicense(ActionListener<? extends AcknowledgedResponse> listener) {
        throw new UnsupportedOperationException("removing a license in serverless is not supported");
    }

    @Override
    public void startBasicLicense(PostStartBasicRequest request, ActionListener<PostStartBasicResponse> listener) {
        throw new UnsupportedOperationException("starting a basic license in serverless is not supported");
    }

    @Override
    public void startTrialLicense(PostStartTrialRequest request, ActionListener<PostStartTrialResponse> listener) {
        throw new UnsupportedOperationException("starting a trial license in serverless is not supported");
    }

    @Override
    protected void doStart() {
        // do nothing
    }

    @Override
    protected void doStop() {
        // do nothing
    }

    @Override
    protected void doClose() throws IOException {
        // do nothing
    }
}
