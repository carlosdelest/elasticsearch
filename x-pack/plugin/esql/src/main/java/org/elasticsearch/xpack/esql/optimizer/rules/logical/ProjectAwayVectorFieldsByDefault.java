/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.List;

public class ProjectAwayVectorFieldsByDefault extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan plan) {

        AttributeSet.Builder builder = AttributeSet.builder();
        plan.forEachDown(EsRelation.class, esRelation -> {
            builder.addAll(esRelation.output().stream().filter(ProjectAwayVectorFieldsByDefault::isVectorFieldAttr).toList());
        });

        collectVectorFieldsAndProjections(plan, builder);

        AttributeSet attrsToRemove = builder.build();
        if (attrsToRemove.isEmpty()) {
            return plan;
        }

        List<Attribute> projectedAttrs = plan.outputSet().stream().filter(a -> attrsToRemove.contains(a) == false).toList();
        return new Project(Source.EMPTY, plan, projectedAttrs);
    }

    private void collectVectorFieldsAndProjections(LogicalPlan plan, AttributeSet.Builder vectorAttrsBuilder) {
        if (plan instanceof EsqlProject project) {
            vectorAttrsBuilder.removeIf(a -> project.projections().contains(a) == false);
        } else if (plan instanceof Project project) {
            vectorAttrsBuilder.removeIf(a -> project.projections().contains(a) == false);
        }
    }

    private static boolean isVectorFieldAttr(Attribute attribute) {
        return attribute instanceof FieldAttribute fieldAttribute && fieldAttribute.dataType() == DataType.DENSE_VECTOR;
    }
}
