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

package co.elastic.elasticsearch.stateless.action;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGenerationTests;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.stream.Collectors;

public class NewCommitNotificationResponseSerializationTests extends AbstractWireSerializingTestCase<NewCommitNotificationResponse> {

    @Override
    protected Writeable.Reader<NewCommitNotificationResponse> instanceReader() {
        return NewCommitNotificationResponse::new;
    }

    @Override
    protected NewCommitNotificationResponse createTestInstance() {
        return new NewCommitNotificationResponse(randomSet(0, 10, PrimaryTermAndGenerationTests::randomPrimaryTermAndGeneration));
    }

    @Override
    protected NewCommitNotificationResponse mutateInstance(NewCommitNotificationResponse instance) throws IOException {
        if (instance.getPrimaryTermAndGenerationsInUse().isEmpty()) {
            return new NewCommitNotificationResponse(randomSet(1, 10, PrimaryTermAndGenerationTests::randomPrimaryTermAndGeneration));
        }

        return new NewCommitNotificationResponse(
            instance.getPrimaryTermAndGenerationsInUse()
                .stream()
                .map(PrimaryTermAndGenerationTests::mutatePrimaryTermAndGeneration)
                .collect(Collectors.toSet())
        );
    }
}
