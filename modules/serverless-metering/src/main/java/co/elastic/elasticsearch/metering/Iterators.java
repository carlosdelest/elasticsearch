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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

// TODO: move to org.elasticsearch.common.collect.Iterators
class Iterators {
    public static <T> Iterator<T> limit(Iterator<? extends T> input, int n) {
        if (input.hasNext()) {
            if (input instanceof LimitIterator<? extends T> limitIterator) {
                return new LimitIterator<>(limitIterator.input, Math.min(limitIterator.limit, n));
            }
            return new LimitIterator<>(input, n);
        } else {
            return Collections.emptyIterator();
        }
    }

    private static final class LimitIterator<T> implements Iterator<T> {
        private final Iterator<? extends T> input;
        private final int limit;
        private int current;

        LimitIterator(Iterator<? extends T> input, int limit) {
            this.input = input;
            this.limit = limit;
        }

        @Override
        public boolean hasNext() {
            return input.hasNext() && current < limit;
        }

        @Override
        public T next() {
            if (current >= limit) {
                throw new NoSuchElementException();
            }
            ++current;
            return input.next();
        }
    }

    public static <T> List<T> toList(Iterator<T> iterator) {
        var list = new ArrayList<T>();
        iterator.forEachRemaining(list::add);
        return list;
    }
}
