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

package co.elastic.elasticsearch.api.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ReindexRequestValidatorTests extends ESTestCase {

    Task task = Mockito.mock(Task.class);
    @Mock
    ActionFilterChain<ActionRequest, ActionResponse> chain;
    @Mock
    ActionListener<ActionResponse> listener;

    public void testRemoteInfoError() {
        var request = new ReindexRequest();
        request.setRemoteInfo(new RemoteInfo("", "", 0, "", new BytesArray("{}"), "", null, Map.of(), TimeValue.ZERO, TimeValue.ZERO));
        var e = expectThrows(IllegalArgumentException.class, () -> runValidator(request));
        assertThat(e.getMessage(), equalTo("reindex.remote is not supported in serverless mode"));
    }

    void runValidator(ActionRequest r) {
        var validator = new ReindexRequestValidator();
        validator.apply(task, ReindexAction.NAME, r, listener, chain);
    }
}
