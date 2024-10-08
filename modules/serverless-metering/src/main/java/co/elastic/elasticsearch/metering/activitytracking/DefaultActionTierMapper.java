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

package co.elastic.elasticsearch.metering.activitytracking;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static co.elastic.elasticsearch.metering.activitytracking.ActionTier.BOTH;
import static co.elastic.elasticsearch.metering.activitytracking.ActionTier.INDEX;
import static co.elastic.elasticsearch.metering.activitytracking.ActionTier.NEITHER;
import static co.elastic.elasticsearch.metering.activitytracking.ActionTier.Rule;
import static co.elastic.elasticsearch.metering.activitytracking.ActionTier.SEARCH;

public class DefaultActionTierMapper implements ActionTier.Mapper {

    /**
     * Since the default rule value is BOTH, all rules below serve to restrict
     * the tracking of an action to a specific tier, or to skip tracking entirely.
     * For example, security actions occur frequently. We do not
     * want to always be tracking in both tiers due to these actions, so there
     * is a rule mapping the action "cluster:admin/xpack/security/" to NEITHER.
     * On the other hand, mapping actions are closely related to index creations.
     * We want to track them, but only as index requests. For this reason,
     * we have the rule mapping "indices:admin/mapping/put" to INDEX.
     */
    public static final List<Rule> RULES = List.of(
        new Rule("indices:data/write/", INDEX),
        new Rule("indices:data/read/", SEARCH),
        new Rule("cluster:admin/xpack/security/", NEITHER),
        new Rule("cluster:admin/component_template/get", NEITHER),
        new Rule("indices:admin/aliases/get", NEITHER),
        new Rule("indices:admin/data_stream/get", NEITHER),
        new Rule("indices:admin/index_template/get", NEITHER),
        new Rule("cluster:admin/ingest/pipeline/get", NEITHER),
        new Rule("indices:admin/create", INDEX),
        new Rule("indices:admin/delete", INDEX),
        new Rule("indices:admin/mapping/put", INDEX),
        new Rule("indices:admin/mapping/auto_put", INDEX),
        new Rule("indices:admin/auto_create", INDEX),
        new Rule("cluster:monitor/main", NEITHER)  // emitted on serverless initial startup
    );

    public static final ActionTier.Mapper INSTANCE = new DefaultActionTierMapper(RULES);
    private final NavigableMap<String, ActionTier> tree = new TreeMap<>();

    DefaultActionTierMapper(List<Rule> rules) {
        for (var rule : rules) {
            tree.put(rule.actionPrefix(), rule.tier());
        }
    }

    @Override
    public ActionTier toTier(String action) {
        // Since a matching rule is a prefix of an action it must be of equal length
        // or shorter than the action. Since a matching rules can be shorter than
        // the action, and the prefixes are sorted in ascending order, we use floor
        // to look for rules that come before the action in sort order.
        var entry = tree.floorEntry(action);

        // Still need to check that the rule is actually a prefix of the action
        if (entry != null && action.startsWith(entry.getKey())) {
            return entry.getValue();
        }
        return BOTH;
    }
}
