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

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DotPrefixValidatorTests extends ESTestCase {
    private final OperatorValidator<?> opV = new OperatorValidator<>();
    private final NonOperatorValidator<?> nonOpV = new NonOperatorValidator<>();

    private static ClusterService clusterService;

    @BeforeClass
    public static void beforeClass() {
        List<String> allowed = new ArrayList<>(DotPrefixValidator.IGNORED_INDEX_PATTERNS_SETTING.getDefault(Settings.EMPTY));
        // Add a new allowed pattern for testing
        allowed.add("\\.potato\\d+");
        Settings settings = Settings.builder()
            .put(DotPrefixValidator.IGNORED_INDEX_PATTERNS_SETTING.getKey(), Strings.collectionToCommaDelimitedString(allowed))
            .build();
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Sets.newHashSet(DotPrefixValidator.VALIDATE_DOT_PREFIXES, DotPrefixValidator.IGNORED_INDEX_PATTERNS_SETTING)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.threadPool()).thenReturn(mock(ThreadPool.class));
    }

    public void testValidation() {

        nonOpV.validateIndices(Set.of("regular"));
        opV.validateIndices(Set.of("regular"));
        assertFails(Set.of(".regular"));
        opV.validateIndices(Set.of(".regular"));
        assertFails(Set.of("first", ".second"));
        assertFails(Set.of("<.regular-{MM-yy-dd}>"));

        // Test ignored names
        nonOpV.validateIndices(Set.of(".elastic-connectors-v1"));
        nonOpV.validateIndices(Set.of(".elastic-connectors-sync-jobs-v1"));
        nonOpV.validateIndices(Set.of(".ml-state"));
        nonOpV.validateIndices(Set.of(".ml-anomalies-unrelated"));

        // Test ignored patterns
        nonOpV.validateIndices(Set.of(".ml-state-21309"));
        nonOpV.validateIndices(Set.of(">.ml-state-21309>"));
        nonOpV.validateIndices(Set.of(".slo-observability.sli-v2"));
        nonOpV.validateIndices(Set.of(".slo-observability.sli-v2.3"));
        nonOpV.validateIndices(Set.of(".slo-observability.sli-v2.3-2024-01-01"));
        nonOpV.validateIndices(Set.of("<.slo-observability.sli-v3.3.{2024-10-16||/M{yyyy-MM-dd|UTC}}>"));
        nonOpV.validateIndices(Set.of(".slo-observability.summary-v2"));
        nonOpV.validateIndices(Set.of(".slo-observability.summary-v2.3"));
        nonOpV.validateIndices(Set.of(".slo-observability.summary-v2.3-2024-01-01"));
        nonOpV.validateIndices(Set.of("<.slo-observability.summary-v3.3.{2024-10-16||/M{yyyy-MM-dd|UTC}}>"));
        nonOpV.validateIndices(Set.of(".entities.v1.latest.builtin_services_from_ecs_data"));
        nonOpV.validateIndices(Set.of(".entities.v92.latest.eggplant.potato"));
        nonOpV.validateIndices(Set.of("<.entities.v12.latest.eggplant-{M{yyyy-MM-dd|UTC}}>"));

        // Test pattern added to the settings
        nonOpV.validateIndices(Set.of(".potato5"));
        nonOpV.validateIndices(Set.of("<.potato5>"));
    }

    private void assertFails(Set<String> indices) {
        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> nonOpV.validateIndices(indices));
        assertThat(error.getMessage(), containsString("name beginning with a dot (.) is not allowed"));
    }

    private static class NonOperatorValidator<R> extends DotPrefixValidator<R> {

        private NonOperatorValidator() {
            super(new ThreadContext(Settings.EMPTY), clusterService);
        }

        @Override
        protected Set<String> getIndicesFromRequest(Object request) {
            return Set.of();
        }

        @Override
        public String actionName() {
            return "";
        }

        @Override
        boolean isOperator() {
            return false;
        }
    }

    private static class OperatorValidator<R> extends NonOperatorValidator<R> {
        @Override
        boolean isOperator() {
            return true;
        }
    }
}
