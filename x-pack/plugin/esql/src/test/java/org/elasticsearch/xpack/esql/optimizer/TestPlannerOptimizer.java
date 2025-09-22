/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.planner.premapper.PreMapper;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryBuilderResolver;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;

import java.io.IOException;
import java.util.Set;

public class TestPlannerOptimizer {
    private final EsqlParser parser;
    private final Analyzer analyzer;
    private final LogicalPlanOptimizer logicalOptimizer;
    private final PhysicalPlanOptimizer physicalPlanOptimizer;
    private final Mapper mapper;
    private final PreMapper preMapper;
    private final Configuration config;

    public TestPlannerOptimizer(Configuration config, Analyzer analyzer) {
        this(config, analyzer, new LogicalPlanOptimizer(new LogicalOptimizerContext(config, FoldContext.small())));
    }

    public TestPlannerOptimizer(Configuration config, Analyzer analyzer, LogicalPlanOptimizer logicalOptimizer) {
        this.analyzer = analyzer;
        this.config = config;
        this.logicalOptimizer = logicalOptimizer;

        parser = new EsqlParser();
        physicalPlanOptimizer = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(config));
        mapper = new Mapper();
        preMapper = new TestPreMapper();
    }

    public PhysicalPlan plan(String query) {
        return plan(query, EsqlTestUtils.TEST_SEARCH_STATS);
    }

    public PhysicalPlan plan(String query, SearchStats stats) {
        return plan(query, stats, analyzer);
    }

    public PhysicalPlan plan(String query, SearchStats stats, Analyzer analyzer) {
        var physical = optimizedPlan(physicalPlan(query, analyzer), stats);
        return physical;
    }

    public PhysicalPlan plan(String query, SearchStats stats, EsqlFlags esqlFlags) {
        return optimizedPlan(physicalPlan(query, analyzer), stats, esqlFlags);
    }

    private PhysicalPlan optimizedPlan(PhysicalPlan plan, SearchStats searchStats) {
        return optimizedPlan(plan, searchStats, new EsqlFlags(true));
    }

    private PhysicalPlan optimizedPlan(PhysicalPlan plan, SearchStats searchStats, EsqlFlags esqlFlags) {
        // System.out.println("* Physical Before\n" + plan);
        var physicalPlan = EstimatesRowSize.estimateRowSize(0, physicalPlanOptimizer.optimize(plan));
        // System.out.println("* Physical After\n" + physicalPlan);
        // the real execution breaks the plan at the exchange and then decouples the plan
        // this is of no use in the unit tests, which checks the plan as a whole instead of each
        // individually hence why here the plan is kept as is

        var logicalTestOptimizer = new LocalLogicalPlanOptimizer(
            new LocalLogicalOptimizerContext(config, FoldContext.small(), searchStats)
        );
        var physicalTestOptimizer = new TestLocalPhysicalPlanOptimizer(
            new LocalPhysicalOptimizerContext(esqlFlags, config, FoldContext.small(), searchStats),
            true
        );
        var l = PlannerUtils.localPlan(physicalPlan, logicalTestOptimizer, physicalTestOptimizer);

        // handle local reduction alignment
        l = PhysicalPlanOptimizerTests.localRelationshipAlignment(l);

        // System.out.println("* Localized DataNode Plan\n" + l);
        return l;
    }

    private PhysicalPlan physicalPlan(String query, Analyzer analyzer) {
        LogicalPlan logical = logicalOptimizer.optimize(analyzer.analyze(parser.createStatement(query, EsqlTestUtils.TEST_CFG)));
        PlainActionFuture<LogicalPlan> preMapperFuture = new PlainActionFuture<>();
        preMapper.preMapper(logical, preMapperFuture);
        return mapper.map(preMapperFuture.actionGet());
    }

    private class TestQueryBuilderResolver extends QueryBuilderResolver {
        TestQueryBuilderResolver() {
            super(EsqlTestUtils.MOCK_TRANSPORT_ACTION_SERVICES);
        }

        @Override
        protected QueryRewriteContext queryRewriteContext(TransportActionServices services, Set<String> indexNames) {
            return new QueryRewriteContext(null, null, null);
        }

        @Override
        protected QueryBuilder rewriteQueryBuilder(QueryRewriteContext ctx, QueryBuilder builder) throws IOException {
            if (builder instanceof MatchQueryBuilder matchQueryBuilder) {
                if (matchQueryBuilder.fieldName().equals("semantic_text")) {
                    return new SemanticQueryBuilder(matchQueryBuilder.fieldName(), (String) matchQueryBuilder.value());
                }
            }

            return builder;
        }
    }

    private class TestPreMapper extends PreMapper {
        TestPreMapper() {
            super(new TestQueryBuilderResolver(), Runnable::run);
        }
    }
}
