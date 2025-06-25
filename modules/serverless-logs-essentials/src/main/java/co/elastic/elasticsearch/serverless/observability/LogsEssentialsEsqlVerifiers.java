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

package co.elastic.elasticsearch.serverless.observability;

import co.elastic.elasticsearch.serverless.constants.ObservabilityTier;
import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.function.BiConsumer;

public class LogsEssentialsEsqlVerifiers implements Verifier.ExtraCheckers {
    @Override
    public List<BiConsumer<LogicalPlan, Failures>> extra(Settings settings) {
        if (ServerlessSharedSettings.OBSERVABILITY_TIER.get(settings) == ObservabilityTier.LOGS_ESSENTIALS) {
            return List.of(LogsEssentialsEsqlVerifiers::disallowCategorize, LogsEssentialsEsqlVerifiers::disallowChangePoint);
        } else {
            return List.of();
        }
    }

    private static void disallowCategorize(LogicalPlan plan, Failures failures) {
        if (plan instanceof Aggregate) {
            plan.forEachExpression(Categorize.class, cat -> failures.add(new Failure(cat, "CATEGORIZE is unsupported")));
        }
    }

    private static void disallowChangePoint(LogicalPlan plan, Failures failures) {
        if (plan instanceof ChangePoint) {
            failures.add(new Failure(plan, "CHANGE_POINT is unsupported"));
        }
    }
}
