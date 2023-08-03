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

package co.elastic.elasticsearch.metering.ingested_size;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.equalTo;

public class MeteringParserTests extends ESTestCase {

    public void testSingleFields() throws IOException {
        // int
        String json = """
            {
            "x": 111
            }
            """;
        assertThat(meter(json), equalTo(Character.SIZE + Long.SIZE));

        // long
        json = """
            {
            "x": 9223372036854775807
            }
            """;
        assertThat(meter(json), equalTo(Character.SIZE + Long.SIZE));

        // !! float is still parsed as double
        json = """
            {
            "x": 1.1
            }
            """;
        assertThat(meter(json), equalTo(Character.SIZE + Double.SIZE));

        // double
        json = """
            {
            "x": 1.7976931348623157E308
            }
            """;
        assertThat(meter(json), equalTo(Character.SIZE + Double.SIZE));

        // boolean
        json = """
            {
            "x": true
            }
            """;
        assertThat(meter(json), equalTo(Character.SIZE + 1));

        // string
        json = """
            {
            "x": "abc"
            }
            """;
        assertThat(meter(json), equalTo(Character.SIZE + Character.SIZE * 3));
    }

    public void testFieldNameLength() throws IOException {
        // 2fields int and string
        String json = """
            {
            "abcd": 111
            }
            """;
        assertThat(meter(json), equalTo(Character.SIZE * 4 + Long.SIZE));
    }

    public void testTwoFields() throws IOException {
        // 2fields int and string
        String json = """
            {
            "x": 111,
            "y": "x"
            }
            """;
        assertThat(meter(json), equalTo(Character.SIZE * 2 + Long.SIZE + Character.SIZE));
    }

    public void testArrayOfInts() throws IOException {
        String json = """
            {
            "x": [1,2,3]
            }
            """;
        assertThat(meter(json), equalTo(Character.SIZE + Long.SIZE * 3));
    }

    public void testSameParsingMethodCalledTwiceDoesNotOvercharge() throws IOException {
        // one field one int
        String json = """
            {
            "x": 1
            }
            """;
        AtomicLong counter = new AtomicLong();

        parse(json, counter, p -> {
            var start = p.nextToken();
            var fieldName = p.nextToken();
            p.currentName();
            p.currentName();
            var field = p.nextToken();
            p.intValue();
            p.intValue();
        });

        assertThat(counter.intValue(), equalTo(Character.SIZE + Long.SIZE));

        json = """
            {
            "x": 1,
            "y": 2
            }
            """;
        counter = new AtomicLong();

        // two fields - two ints
        parse(json, counter, p -> {
            var start = p.nextToken();
            var fieldName1 = p.nextToken();
            p.currentName();
            p.currentName();
            var field1 = p.nextToken();
            p.intValue();
            p.intValue();

            var fieldName2 = p.nextToken();
            p.currentName();
            p.currentName();
            var field2 = p.nextToken();
            p.intValue();
            p.intValue();
        });

        assertThat(counter.intValue(), equalTo(Character.SIZE * 2 + Long.SIZE * 2));
    }

    public void testAllowingToTryParseAgainButChargingForTheFirstAttempt() throws IOException {
        // see IpFieldMapper#parseCreateField

        String json = """
            {
            "x": 1
            }
            """;
        AtomicLong counter = new AtomicLong();

        parse(json, counter, p -> {
            var start = p.nextToken();
            var fieldName = p.nextToken();
            p.currentName();
            p.currentName();
            var field = p.nextToken();
            p.text(); // charges for text
            p.intValue();// does not throws exception, does not charge
        });
        assertThat(counter.intValue(), equalTo(Character.SIZE + Character.SIZE));

    }

    public void testCurrentNameInterleaving() throws IOException {
        // one field one int
        String json = """
            {
            "x": 1
            }
            """;
        AtomicLong counter = new AtomicLong();

        parse(json, counter, p -> {
            var start = p.nextToken();
            var fieldName = p.nextToken();
            p.currentName();
            var field = p.nextToken();
            p.currentName(); // should not be charging
            p.intValue();
        });

        assertThat(counter.intValue(), equalTo(Character.SIZE + Long.SIZE));

        // nested interleaving
        json = """
            {
                "x": {
                    "x": 1
                }
            }
            """;
        counter = new AtomicLong();

        parse(json, counter, p -> {
            var start = p.nextToken();
            var xName1 = p.nextToken();
            p.currentName();
            var object = p.nextToken();
            p.currentName(); // should not be charging - still x1
            var xName2 = p.nextToken();
            p.currentName();
            var value = p.nextToken();
            p.intValue();
            p.currentName();
        });

        assertThat(counter.intValue(), equalTo(Character.SIZE * 2 + Long.SIZE));
    }

    public void testNestedFields() throws IOException {
        // one field one int
        String json = """
            {
                "x": {
                    "y" : 1
                }
            }
            """;
        AtomicLong counter = new AtomicLong();

        parse(json, counter, p -> {
            var start = p.nextToken();
            var xFieldName = p.nextToken();
            p.currentName();// x charged
            p.currentName();
            var startObject = p.nextToken();
            var yFieldName = p.nextToken();
            String yName = p.currentName();// y charged
            XContentParser.Token value = p.nextToken();
            p.intValue(); // number charged
            p.intValue();
        });

        assertThat(counter.intValue(), equalTo(Character.SIZE * 2 + Long.SIZE));
        assertThat(counter.intValue(), equalTo(meter(json)));
    }

    public void testJsonVsCbor() throws IOException {
        // 2fields int and string
        String json = """
            {
            "abcd": 111
            }
            """;
        int meteredJson = Character.SIZE * 4 + Long.SIZE;
        assertThat(meter(json), equalTo(meteredJson));

        // the same should be metered in cbor
        BytesArray bytesArray = convertTo(json, XContentFactory::cborBuilder);

        AtomicLong counter = new AtomicLong();
        parse(bytesArray, XContentType.CBOR, counter, p -> p.map());
        assertThat(counter.intValue(), equalTo(meteredJson));
    }

    private static BytesArray convertTo(String json, CheckedSupplier<XContentBuilder, IOException> xContentBuilderSupplier)
        throws IOException {
        try (
            XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, new BytesArray(json), XContentType.JSON)
        ) {

            parser.nextToken();
            XContentBuilder builder = xContentBuilderSupplier.get();

            builder.copyCurrentStructure(parser);
            return new BytesArray(BytesReference.toBytes(BytesReference.bytes(builder)));
        }
    }

    private static int meter(String json) throws IOException {
        AtomicLong counter = new AtomicLong();

        parse(json, counter, p -> p.map());
        return counter.intValue();
    }

    private static void parse(String json, AtomicLong counter, CheckedConsumer<XContentParser, IOException> parserConsumer)
        throws IOException {
        parse(new BytesArray(json), XContentType.JSON, counter, parserConsumer);
    }

    private static void parse(
        BytesArray bytesArray,
        XContentType xContentType,
        AtomicLong counter,
        CheckedConsumer<XContentParser, IOException> parserConsumer
    ) throws IOException {
        try (
            XContentParser parser = new MeteringParser(
                XContentHelper.createParser(XContentParserConfiguration.EMPTY, bytesArray, xContentType),
                counter
            )
        ) {
            parserConsumer.accept(parser);
        }
    }
}
