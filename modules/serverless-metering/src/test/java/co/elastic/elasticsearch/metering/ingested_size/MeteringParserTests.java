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

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.equalTo;

public class MeteringParserTests extends ESTestCase {
    private final XContentType xContentType;
    private static final int ASCII_SIZE = 1;// in bytes
    private static final int NUMBER_SIZE = Long.BYTES;

    @ParametersFactory
    public static List<Object[]> params() {
        return Arrays.stream(XContentType.values()).map(xct -> new Object[] { xct }).toList();
    }

    public MeteringParserTests(XContentType xContentType) {
        this.xContentType = xContentType;
    }

    public void testCharacters() throws IOException {
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"a\""), ASCII_SIZE, XContentParser::text);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"abcd\""), ASCII_SIZE * 4, XContentParser::textOrNull);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("null"), 0, XContentParser::textOrNull);
    }

    public void testNonAsciiCharacters() throws IOException {
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"©\""), 2, XContentParser::text);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"€\""), 3, XContentParser::textOrNull);

        // \xE2\x98\x95 hot beverage
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"☕\""), 3, XContentParser::textOrNull);
        // \xF0\x9F\x92\xA9 pile of poo
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"💩\""), 4, XContentParser::textOrNull);
    }

    public void testBooleanRepresentations() throws IOException {
        CheckedConsumer<XContentParser, IOException> p = XContentParser::booleanValue;
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("true"), 1, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("false"), 1, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"true\""), ASCII_SIZE * 4, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"false\""), ASCII_SIZE * 5, p);
    }

    public void testShortRepresentations() throws IOException {
        CheckedConsumer<XContentParser, IOException> p = XContentParser::shortValue;

        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("1"), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("111"), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"1\""), ASCII_SIZE * 1, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"111\""), ASCII_SIZE * 3, p);

        var v = String.valueOf(Short.MAX_VALUE);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(v), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"" + v + "\""), textSize(v), p);

        v = String.valueOf(Short.MIN_VALUE);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(v), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"" + v + "\""), textSize(v), p);
    }

    public void testIntRepresentations() throws IOException {
        CheckedConsumer<XContentParser, IOException> p = XContentParser::intValue;

        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("1"), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("111"), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"1\""), ASCII_SIZE * 1, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"111\""), ASCII_SIZE * 3, p);

        var v = String.valueOf(Integer.MAX_VALUE);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(v), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"" + v + "\""), textSize(v), p);

        v = String.valueOf(Integer.MIN_VALUE);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(v), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"" + v + "\""), textSize(v), p);
    }

    public void testLongRepresentations() throws IOException {
        CheckedConsumer<XContentParser, IOException> p = XContentParser::longValue;

        var v = String.valueOf((long) Integer.MAX_VALUE + 1);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(v), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"" + v + "\""), textSize(v), p);

        v = String.valueOf(Long.MIN_VALUE);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(v), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"" + v + "\""), textSize(v), p);

        v = String.valueOf(Long.MAX_VALUE);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(v), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"" + v + "\""), textSize(v), p);
    }

    public void testUnsignedLongRepresentations() throws IOException {
        // covering biginteger
        var v = "9223372036854775808"; // Long.MAX_VALUE (2^63) + 1
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(v), NUMBER_SIZE, XContentParser::numberValue);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"" + v + "\""), textSize(v), XContentParser::text);

        v = "18446744073709551615"; // 2^64 - 1 max unsigned long value
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(v), NUMBER_SIZE, XContentParser::numberValue);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"" + v + "\""), textSize(v), XContentParser::text);
    }

    public void testFloatRepresentations() throws IOException {
        CheckedConsumer<XContentParser, IOException> p = XContentParser::floatValue;

        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("123.123"), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"1.0\""), textSize("1.0"), p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"123.123\""), textSize("123.123"), p);

        var v = String.valueOf(Float.MIN_VALUE);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(v), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"" + v + "\""), textSize(v), p);

        v = String.valueOf(Float.MAX_VALUE);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(v), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"" + v + "\""), textSize(v), p);
    }

    public void testDoubleRepresentations() throws IOException {
        CheckedConsumer<XContentParser, IOException> p = XContentParser::doubleValue;

        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("123.123"), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"1.0\""), textSize("1.0"), p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"123.123\""), textSize("123.123"), p);

        var v = String.valueOf(Float.MIN_VALUE);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(v), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"" + v + "\""), textSize(v), p);

        v = String.valueOf(Float.MAX_VALUE);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(v), NUMBER_SIZE, p);
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField("\"" + v + "\""), textSize(v), p);
    }

    public void testBinaryFieldsAsString() throws IOException {
        String base64 = "U29tZSBiaW5hcnkgYmxvYg==";
        String fieldValue = "\"" + base64 + "\"";
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(fieldValue), textSize(base64), XContentParser::binaryValue);
    }

    public void testBinaryFieldFromBuilder() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.contentBuilder(xContentType);
        xContentBuilder.startObject();
        byte[] inputBytes = Base64.getDecoder().decode("U29tZSBiaW5hcnkgYmxvYg==");
        assertThat(inputBytes.length, equalTo(16));
        xContentBuilder.field("x", inputBytes);
        xContentBuilder.endObject();

        BytesArray bytesToParse = new BytesArray(BytesReference.toBytes(BytesReference.bytes(xContentBuilder)));

        CheckedConsumer<XContentParser, IOException> parser = p -> {
            var start = p.nextToken();
            var xFieldName = p.nextToken(); // x charged
            p.currentName();
            var binary = p.nextToken();
            byte[] bytes = p.binaryValue();
            assertThat(bytes, Matchers.is(inputBytes));
        };

        int parserMetered = parserWithXContentParser(bytesToParse, parser);
        int mapMetered = parserWithXContentParser(bytesToParse, p -> p.map());

        assertThat("bulk vs ingest do not match", parserMetered, equalTo(mapMetered));
        // in fact we have sent less data via cbor or smile but we calculate the base64 length
        assertThat(mapMetered, equalTo("U29tZSBiaW5hcnkgYmxvYg==".length() * ASCII_SIZE + ASCII_SIZE));
    }

    public void testDisabledFields() throws IOException {
        // when a disabled field is used, a skipChildren is being called. we want to meter this
        String subJson = """
            {
            "y": "123"
            }
            """; // 4 * character size
        assertSingleFieldParsingSameForParserAndMap(jsonWithSingleField(subJson), 4 * ASCII_SIZE, XContentParser::skipChildren);
    }

    public void testTwoFieldsOfDifferentFieldNameLength() throws IOException {
        // 2fields int and string
        String json = """
            {
            "abcd": "xyz",
            "efghijk" : 1
            }
            """;

        CheckedConsumer<XContentParser, IOException> parser = p -> {
            var start = p.nextToken();
            var abcdFieldName = p.nextToken(); // abcd charged
            p.currentName();
            var number111 = p.nextToken(); // "xyz" charged
            p.text();
            var efghijkFieldName = p.nextToken(); // efghijk charged
            p.currentName();
            var number1 = p.nextToken(); // 1 charged
            p.intValue();
        };

        assertParsingSameForParserAndMap(
            json,
            "abcd".length() * ASCII_SIZE + "xyz".length() * ASCII_SIZE + "efghijk".length() * ASCII_SIZE + NUMBER_SIZE,
            parser
        );
    }

    public void testArrayOfInts() throws IOException {
        String json = """
            {
            "x": [1,2,3]
            }
            """;
        CheckedConsumer<XContentParser, IOException> parser = p -> {
            var start = p.nextToken();
            var xFieldName = p.nextToken(); // x charged
            p.currentName();
            var arrayStart = p.nextToken();
            for (int i = 0; i < 3; i++) {
                var numberI = p.nextToken(); // number charged
                p.intValue();
            }
            var arrayEnd = p.nextToken();
        };
        assertParsingSameForParserAndMap(json, ASCII_SIZE + 3 * NUMBER_SIZE, parser);
    }

    public void testSameParsingMethodCalledTwiceDoesNotOvercharge() throws IOException {
        // one field one int
        String json = """
            {
            "x": 1
            }
            """;

        CheckedConsumer<XContentParser, IOException> parser = p -> {
            var start = p.nextToken();
            var fieldName = p.nextToken();
            p.currentName();
            p.currentName();
            var field = p.nextToken();
            p.intValue();
            p.intValue();
        };
        assertParsingSameForParserAndMap(json, ASCII_SIZE + NUMBER_SIZE, parser);

        json = """
            {
            "x": 1,
            "y": 2
            }
            """;

        // two fields - two ints
        parser = p -> {
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
        };
        assertParsingSameForParserAndMap(json, ASCII_SIZE * 2 + NUMBER_SIZE * 2, parser);
    }

    public void testFallbackParsingIsNotCharging() throws IOException {
        // see IpFieldMapper#parseCreateField
        String json = """
            {
            "x": "1.1.1"
            }
            """;
        AtomicLong counter = new AtomicLong();

        CheckedConsumer<XContentParser, IOException> parser = p -> {
            var start = p.nextToken();
            var fieldName = p.nextToken(); // charges for text
            p.currentName();
            p.currentName();
            var field = p.nextToken(); // charges for text
            p.text();
            // let's pretend the text is invalid, so we would like to store it if storeIgnored is enabled
            p.text();
        };
        assertParsingSameForParserAndMap(json, ASCII_SIZE * 5 + ASCII_SIZE, parser);
    }

    public void testWrongTypeMethodCalled() throws IOException {
        String json = """
            {
            "x": 123
            }
            """;
        AtomicLong counter = new AtomicLong();

        CheckedConsumer<XContentParser, IOException> parser = p -> {
            var start = p.nextToken();
            var fieldName = p.nextToken();
            p.currentName();
            p.currentName();
            var field = p.nextToken();  // charges for number
            p.text(); // does not charge
        };
        assertParsingSameForParserAndMap(json, ASCII_SIZE + NUMBER_SIZE, parser);
    }

    public void testIgnoreMalformed() throws IOException {
        String json = """
            {
            "x": 1
            }
            """;
        AtomicLong counter = new AtomicLong();

        CheckedConsumer<XContentParser, IOException> parser = p -> {
            var start = p.nextToken();
            var fieldName = p.nextToken();
            p.currentName();
            p.currentName();
            var field = p.nextToken(); // charges for int
            p.text(); // does not charge
            p.intValue();
        };
        assertParsingSameForParserAndMap(json, ASCII_SIZE + NUMBER_SIZE, parser);
    }

    public void testCurrentNameInterleaving() throws IOException {
        // one field one int
        String json = """
            {
            "x": 1
            }
            """;

        CheckedConsumer<XContentParser, IOException> parser = p -> {
            var start = p.nextToken();
            var fieldName = p.nextToken(); // charging char
            p.currentName();
            var field = p.nextToken(); // charging number
            p.currentName();
            p.intValue();
        };
        assertParsingSameForParserAndMap(json, ASCII_SIZE + NUMBER_SIZE, parser);

        // nested interleaving
        json = """
            {
                "x": {
                    "x": 1
                }
            }
            """;

        parser = p -> {
            var start = p.nextToken();
            var xName1 = p.nextToken(); // charge for 'x'
            p.currentName();
            var startObject = p.nextToken(); // start object, does not charge
            p.currentName();
            var xName2 = p.nextToken(); // charge for 'x' (nested)
            p.currentName();
            var value = p.nextToken();  // charge for int
            p.intValue();
            p.currentName();
        };
        assertParsingSameForParserAndMap(json, ASCII_SIZE * 2 + NUMBER_SIZE, parser);
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

        CheckedConsumer<XContentParser, IOException> parser = p -> {
            var start = p.nextToken();
            var xFieldName = p.nextToken();// x charged
            p.currentName();
            p.currentName();
            var startObject = p.nextToken(); // start object. does not charge
            var yFieldName = p.nextToken(); // y charged
            String yName = p.currentName();
            XContentParser.Token value = p.nextToken(); // number charged
            p.intValue();
            p.intValue();
        };
        assertParsingSameForParserAndMap(json, ASCII_SIZE * 2 + NUMBER_SIZE, parser);
    }

    public void testNestedFieldsAndSkipChildren() throws IOException {
        String json = """
            {
                "x": {
                    "y" : 123
                },
                "z": 1
            }
            """; // x's subobject will be skipped

        CheckedConsumer<XContentParser, IOException> parser = p -> {
            var start = p.nextToken();
            var xFieldName = p.nextToken(); // x charged
            p.currentName();
            p.currentName();
            var startObject = p.nextToken(); // start object, doest not charge
            p.skipChildren(); // will traverse subobject and charge for y and 123, will consume input up to endObject token
            var zFieldName = p.nextToken(); // z charged
            p.currentName();
            var zValue = p.nextToken();// long charged
            p.intValue();
        };

        assertParsingSameForParserAndMap(json, ASCII_SIZE * 3 + 2 * NUMBER_SIZE, parser);
    }

    public void testMultipleNestedFieldsAndSkipChildren() throws IOException {
        String json = """
            {
                "x": {
                    "y" : {
                     "z": {
                        "v": "a"
                     }
                    }
                },
                "w": "b"
            }
            """; // x's subobject will be skipped

        CheckedConsumer<XContentParser, IOException> parser = p -> {
            var start = p.nextToken();
            var xFieldName = p.nextToken(); // x charged
            p.currentName();
            p.currentName();
            var startObject = p.nextToken();
            p.skipChildren(); // will traverse subobject and charge for y,z,v and a, will consume input up to endObject token
            var wFieldName = p.nextToken(); // w charged
            p.currentName();
            var wValue = p.nextToken(); // "b" charged
            p.text();
        };
        assertParsingSameForParserAndMap(json, ASCII_SIZE * 7, parser);
    }

    public void testSkipChildrenWithBinaryField() throws IOException {
        String json = """
            {
                "x": {
                    "y" : "QQ=="
                }
            }
            """; // x's subobject will be skipped

        CheckedConsumer<XContentParser, IOException> parser = p -> {
            var start = p.nextToken();
            var xFieldName = p.nextToken(); // x charged
            p.currentName();
            p.currentName();
            var startObject = p.nextToken(); // will traverse subobject and charge for y and binary data
            p.skipChildren();
        };

        assertParsingSameForParserAndMap(json, ASCII_SIZE * 6, parser);
    }

    private static String jsonWithSingleField(String value) {
        var json = Strings.format("""
            {
            "x": %s
            }
            """, value);
        return json;
    }

    public void assertSingleFieldParsingSameForParserAndMap(
        String json,
        int expectedSize,
        CheckedConsumer<XContentParser, IOException> valueParser
    ) throws IOException {
        CheckedConsumer<XContentParser, IOException> parser = singleFieldParser(valueParser);
        assertParsingSameForParserAndMap(json, expectedSize + ASCII_SIZE/*single field name*/, parser);
    }

    public void assertParsingSameForParserAndMap(String json, int expectedSize, CheckedConsumer<XContentParser, IOException> parser)
        throws IOException {
        BytesArray bytesToParse = convertTo(json, () -> XContentFactory.contentBuilder(xContentType));
        int parserMetered = parserWithXContentParser(bytesToParse, parser);
        int mapMetered = parserWithXContentParser(bytesToParse, p -> p.map());

        assertThat("bulk vs ingest do not match", parserMetered, equalTo(mapMetered));
        assertThat(mapMetered, equalTo(expectedSize));
    }

    private static CheckedConsumer<XContentParser, IOException> singleFieldParser(
        CheckedConsumer<XContentParser, IOException> fieldParser
    ) {
        CheckedConsumer<XContentParser, IOException> parser = p -> {
            var start = p.nextToken();
            var xFieldName = p.nextToken();
            p.currentName();// x charged
            var valueToken = p.nextToken();
            fieldParser.accept(p);// pare a field value, we don't care about value
        };
        return parser;
    }

    private static int textSize(String text) {
        return text.length() * ASCII_SIZE;
    }

    private BytesArray convertTo(String json, CheckedSupplier<XContentBuilder, IOException> xContentBuilderSupplier) throws IOException {
        try (
            XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, new BytesArray(json), XContentType.JSON)
        ) {
            XContentBuilder builder = xContentBuilderSupplier.get();
            builder.copyCurrentStructure(parser);
            return new BytesArray(BytesReference.toBytes(BytesReference.bytes(builder)));
        }
    }

    public int parseWithXContentUtilMap(String json) throws IOException {
        return parserWithXContentParser(new BytesArray(json), p -> p.map());
    }

    private int parserWithXContentParser(BytesArray bytesArray, CheckedConsumer<XContentParser, IOException> parser) throws IOException {
        AtomicLong counter = new AtomicLong();
        parse(bytesArray, xContentType, counter, parser);

        return counter.intValue();
    }

    public static void parseJson(String json, AtomicLong counter, CheckedConsumer<XContentParser, IOException> parserConsumer)
        throws IOException {
        parse(new BytesArray(json), XContentType.JSON, counter, parserConsumer);
    }

    public static void parse(
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
