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

import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class ActionTierMapperTests extends ESTestCase {
    private static final List<ActionTier.Rule> rules = List.of(
        new ActionTier.Rule("indices:data/write/", ActionTier.SEARCH),
        new ActionTier.Rule("indices:data/multiple/match/test/a/b", ActionTier.SEARCH),
        new ActionTier.Rule("indices:data/multiple/match/test/a", ActionTier.INDEX)
    );

    private static final ActionTier.Mapper mapper = new DefaultActionTierMapper(rules);

    public void testDefaultTier() {
        assertEquals(mapper.toTier("indices:data/not/present/rule"), ActionTier.BOTH);
    }

    public void testRuleAndActionEqual() {
        assertEquals(mapper.toTier("indices:data/write/"), ActionTier.SEARCH);
    }

    public void testRuleIsPrefixOfAction() {
        assertEquals(mapper.toTier("indices:data/write/get"), ActionTier.SEARCH);
    }

    public void testMultipleMatchLongerMatchWins() {
        assertEquals(mapper.toTier("indices:data/multiple/match/test/a/b/c"), ActionTier.SEARCH);
        assertEquals(mapper.toTier("indices:data/multiple/match/test/a"), ActionTier.INDEX);
    }
}
