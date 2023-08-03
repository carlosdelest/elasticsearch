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

import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is an XContentParser that is performing metering.
 * the metering is taking into account field names and field values.
 * The structure, format is ignored.
 *
 * Decimals (int, long) are all metered with same value (long bit size)
 * Floating points (float, double) are metered with double bit size
 * field names are metered with length * char bit size
 * text fields are length * char bit size
 */
public class MeteringParser extends AbstractXContentParser {
    private final XContentParser delegate;
    private final AtomicLong counter;

    private boolean tokenValueCharged = false;
    private boolean currentNameCharged = false;

    /**
     * A function to accumulate total size of the document
     * it takes into account that some methods (i.e. currentName) are called multiple times
     * without moving a token. It does not overcharge for this
     *
     * It also makes sure that we don't have a code that calls different typed methods
     * (like intValue, doubleValue) on the same token. This will throw an AssertionError
     *
     * @param sizeInBits amount of bits corresponding to a type and size
     */
    private void charge(long sizeInBits) {
        if (tokenValueCharged == false) {
            counter.addAndGet(sizeInBits);
            tokenValueCharged = true;
        }
    }

    public MeteringParser(XContentParser xContentParser, AtomicLong counter) {
        super(xContentParser.getXContentRegistry(), xContentParser.getDeprecationHandler(), xContentParser.getRestApiVersion());
        this.delegate = xContentParser;
        this.counter = counter;
    }

    @Override
    public XContentType contentType() {
        return delegate.contentType();
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        delegate.allowDuplicateKeys(allowDuplicateKeys);
    }

    @Override
    public Token nextToken() throws IOException {
        Token token = delegate.nextToken();
        if (token != Token.START_OBJECT && token != Token.END_OBJECT && token != Token.START_ARRAY && token != Token.END_ARRAY) {
            // next field or value
            tokenValueCharged = false;
        }
        if (token == Token.FIELD_NAME) {
            currentNameCharged = false;
        }

        return token;
    }

    /**
     * when a parser is defined to use this method and will be skipping children we cannot charge for them.
     * Those will not be stored as fields in lucene document after all.
     * NOTE! however if the same document is sent over via ingest pipeline we will charge for those fields
     */
    @Override
    public void skipChildren() throws IOException {
        delegate.skipChildren();
    }

    @Override
    public Token currentToken() {
        return delegate.currentToken();
    }

    /**
     * currentName() method is often interleaved between different other calls, even after moving a token.
     * we only want to charge for a field name once.
     */
    @Override
    public String currentName() throws IOException {
        String currentName = delegate.currentName();
        if (currentNameCharged == false) {
            charge(currentName.length() * Character.SIZE);
            currentNameCharged = true;
        }

        return currentName;
    }

    @Override
    public String text() throws IOException {
        String text = delegate.text();
        charge(text.length() * Character.SIZE);
        return text;
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        CharBuffer charBuffer = delegate.charBuffer();
        charge(charBuffer.length() * Character.SIZE);
        return charBuffer;
    }

    @Override
    public Object objectText() throws IOException {
        // TODO how do we measure this?
        Object o = delegate.objectText();
        charge(String.valueOf(o).length() * Character.SIZE);
        return o;
    }

    @Override
    public Object objectBytes() throws IOException {
        // TODO how do we measure this? AbstractQueryBuilder#maybeConvertToBytesRef
        return delegate.objectBytes();
    }

    @Override
    public boolean hasTextCharacters() {
        return delegate.hasTextCharacters();
    }

    @Override
    public char[] textCharacters() throws IOException {
        char[] chars = delegate.textCharacters();
        charge(chars.length * Character.SIZE);
        return chars;
    }

    @Override
    public int textLength() throws IOException {
        return delegate.textLength();
    }

    @Override
    public int textOffset() throws IOException {
        return delegate.textOffset();
    }

    @Override
    public Number numberValue() throws IOException {
        Number number = delegate.numberValue();
        /*TODO discuss how we charge here
         */
        switch (delegate.numberType()) {
            case INT:
                charge(Long.SIZE);
                break;
            case BIG_INTEGER:
                charge(((BigInteger) number).bitLength());
                break;
            case LONG:
                charge(Long.SIZE);
                break;
            case FLOAT:
                charge(Double.SIZE);
                break;
            case DOUBLE:
                charge(Double.SIZE);
                break;
            case BIG_DECIMAL:
                charge(((BigDecimal) number).toBigInteger().bitLength());
        }
        return number;
    }

    @Override
    public NumberType numberType() throws IOException {
        return delegate.numberType();
    }

    @Override
    public byte[] binaryValue() throws IOException {
        byte[] bytes = delegate.binaryValue();
        charge(bytes.length * Byte.SIZE);
        return bytes;
    }

    @Override
    public XContentLocation getTokenLocation() {
        return delegate.getTokenLocation();
    }

    @Override
    protected boolean doBooleanValue() throws IOException {
        charge(1);
        return delegate.booleanValue();
    }

    @Override
    protected short doShortValue() throws IOException {
        charge(Short.SIZE);
        return delegate.shortValue();
    }

    @Override
    protected int doIntValue() throws IOException {
        charge(Long.SIZE);
        return delegate.intValue();
    }

    @Override
    protected long doLongValue() throws IOException {
        charge(Long.SIZE);
        return delegate.longValue();
    }

    @Override
    protected float doFloatValue() throws IOException {
        charge(Double.SIZE);
        return delegate.floatValue();
    }

    @Override
    protected double doDoubleValue() throws IOException {
        charge(Double.SIZE);
        return delegate.doubleValue();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
