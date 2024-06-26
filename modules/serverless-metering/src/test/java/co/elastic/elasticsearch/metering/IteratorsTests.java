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

package co.elastic.elasticsearch.metering;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class IteratorsTests extends ESTestCase {

    public void testLimitOnEmptyIterator() {
        var result = Iterators.limit(Collections.emptyIterator(), 10);
        assertThat(result.hasNext(), is(false));
        assertThat(Iterators.toList(result), is(empty()));
    }

    public void testLimitOnNonEmptyIterator() {
        var list = List.of(1, 2, 3);
        var result = Iterators.limit(list.iterator(), 10);
        assertThat(result.hasNext(), is(true));
        assertThat(Iterators.toList(result), contains(1, 2, 3));

        result = Iterators.limit(list.iterator(), 2);
        assertThat(result.hasNext(), is(true));
        assertThat(Iterators.toList(result), contains(1, 2));

        result = Iterators.limit(list.iterator(), 0);
        assertThat(result.hasNext(), is(false));
        assertThat(Iterators.toList(result), is(empty()));
    }

    public void testComposingLimit() {
        var list = List.of(1, 2, 3, 4, 5);
        var result = Iterators.limit(list.iterator(), 10);
        assertThat(result.hasNext(), is(true));
        assertThat(Iterators.toList(result), contains(1, 2, 3, 4, 5));

        result = Iterators.limit(result, 0);
        assertThat(result.hasNext(), is(false));
        assertThat(Iterators.toList(result), is(empty()));

        result = Iterators.limit(result, 2);
        assertThat(result.hasNext(), is(false));
    }
}
