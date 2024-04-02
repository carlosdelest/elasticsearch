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

import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is an XContentParser that is performing metering.
 * the metering is taking into account field names and field values.
 * The structure, format is ignored.
 * <p>
 * Numbers (float, double, long, int) are all metered with same value (long 8 bytes)
 * field names and text are metered with the number of bytes when encoded in utf-8
 * (1byte for ascii, 2 bytes for bmf chars, 3 and 4bytes for supplementary character set)
 */
public class MeteringParser extends AbstractXContentParser {
    private final XContentParser delegate;
    private final AtomicLong counter;

    /**
     * A function to accumulate total size of the document
     * based on the JSONS's token type it will add size for the type.
     * This method is called by nextToken method which is only called once per each token.
     */
    private void charge(Token token) throws IOException {
        if (token != null) {
            var sizeInBytes = switch (token) {
                case FIELD_NAME -> calculateTextLength(CharBuffer.wrap(currentName()));
                case VALUE_STRING -> calculateTextLength(CharBuffer.wrap(textCharacters(), textOffset(), textLength()));
                case VALUE_EMBEDDED_OBJECT -> calculateBase64Length(binaryValue().length);
                case VALUE_NUMBER -> Long.BYTES;
                case VALUE_BOOLEAN -> 1;
                default -> 0;
            };
            counter.addAndGet(sizeInBytes);
        }
    }

    private int calculateTextLength(CharBuffer charBuffer) throws IOException {
        int byteLength = 0;

        for (int i = 0; i < charBuffer.length(); i++) {
            char c = charBuffer.get(i);
            if (c <= 127) {
                byteLength += 1;
            } else if (c <= 2047) {
                byteLength += 2;
            } else if (Character.isHighSurrogate(c)) {
                // assuming valid encoded bytes, this is followed by a low surrogate
                byteLength += 4;
                i++;
            } else {
                byteLength += 3;
            }
        }

        return byteLength;
    }

    public MeteringParser(XContentParser xContentParser, AtomicLong counter) {
        super(xContentParser.getXContentRegistry(), xContentParser.getDeprecationHandler(), xContentParser.getRestApiVersion());
        Objects.requireNonNull(xContentParser);
        Objects.requireNonNull(counter);
        this.delegate = xContentParser;
        this.counter = counter;
    }

    protected XContentParser delegate() {
        return delegate;
    }

    @Override
    public Token nextToken() throws IOException {
        Token token = delegate().nextToken();
        charge(token);
        return token;
    }

    /**
     * when a parser is defined to use this method (field has a ignored=true) we still want to charge for those.
     * The parsing should iterate all the token as if the field was not ignored.
     */
    @Override
    public void skipChildren() throws IOException {
        // reimplementing the ParserMinimalBase#skipChildren()
        Token currentToken = currentToken();
        if (currentToken != Token.START_OBJECT && currentToken != Token.START_ARRAY) {
            return;
        }
        int open = 1;
        while (true) {
            Token t = nextToken();
            if (t == null) {
                return;
            }
            switch (t) {
                case START_OBJECT:
                case START_ARRAY:
                    ++open;
                    break;
                case END_OBJECT:
                case END_ARRAY:
                    if (--open == 0) {
                        return;
                    }
                    break;
                default:
                    break;
            }
        }
    }

    private long calculateBase64Length(int n) {
        /*
        this calculates the length of a base64 (padded) encoded string for a given byte array length N
        the calculation is as follows:
        every 3 bytes of byte array are encoded in 4characters blocks
        if blocks are not fully populated by a byte array (byte array length is not divisible by 3) the remaining is padded with =
        Therefore we need a ceil (n/3)
        (n+2)/3 is an equivalent of ceil(n/3)
         */
        return 4 * (n + 2) / 3;
    }

    @Override
    public XContentType contentType() {
        return delegate().contentType();
    }

    @Override
    public void allowDuplicateKeys(boolean allowDuplicateKeys) {
        delegate().allowDuplicateKeys(allowDuplicateKeys);
    }

    @Override
    public Token currentToken() {
        return delegate().currentToken();
    }

    @Override
    public String currentName() throws IOException {
        return delegate().currentName();
    }

    @Override
    public String text() throws IOException {
        return delegate().text();
    }

    @Override
    public CharBuffer charBufferOrNull() throws IOException {
        return delegate().charBufferOrNull();
    }

    @Override
    public CharBuffer charBuffer() throws IOException {
        return delegate().charBuffer();
    }

    @Override
    public Object objectText() throws IOException {
        return delegate().objectText();
    }

    @Override
    public Object objectBytes() throws IOException {
        return delegate().objectBytes();
    }

    @Override
    public boolean hasTextCharacters() {
        return delegate().hasTextCharacters();
    }

    @Override
    public char[] textCharacters() throws IOException {
        return delegate().textCharacters();
    }

    @Override
    public int textLength() throws IOException {
        return delegate().textLength();
    }

    @Override
    public int textOffset() throws IOException {
        return delegate().textOffset();
    }

    @Override
    public Number numberValue() throws IOException {
        return delegate().numberValue();
    }

    @Override
    public NumberType numberType() throws IOException {
        return delegate().numberType();
    }

    @Override
    public short shortValue(boolean coerce) throws IOException {
        return delegate().shortValue(coerce);
    }

    @Override
    public int intValue(boolean coerce) throws IOException {
        return delegate().intValue(coerce);
    }

    @Override
    public long longValue(boolean coerce) throws IOException {
        return delegate().longValue(coerce);
    }

    @Override
    public float floatValue(boolean coerce) throws IOException {
        return delegate().floatValue(coerce);
    }

    @Override
    public double doubleValue(boolean coerce) throws IOException {
        return delegate().doubleValue(coerce);
    }

    @Override
    public short shortValue() throws IOException {
        return delegate().shortValue();
    }

    @Override
    public int intValue() throws IOException {
        return delegate().intValue();
    }

    @Override
    public long longValue() throws IOException {
        return delegate().longValue();
    }

    @Override
    public float floatValue() throws IOException {
        return delegate().floatValue();
    }

    @Override
    public double doubleValue() throws IOException {
        return delegate().doubleValue();
    }

    @Override
    public boolean isBooleanValue() throws IOException {
        return delegate().isBooleanValue();
    }

    @Override
    public boolean booleanValue() throws IOException {
        return delegate().booleanValue();
    }

    @Override
    public byte[] binaryValue() throws IOException {
        return delegate().binaryValue();
    }

    @Override
    public XContentLocation getTokenLocation() {
        return delegate().getTokenLocation();
    }

    @Override
    public <T> T namedObject(Class<T> categoryClass, String name, Object context) throws IOException {
        return delegate().namedObject(categoryClass, name, context);
    }

    @Override
    public NamedXContentRegistry getXContentRegistry() {
        return delegate().getXContentRegistry();
    }

    @Override
    public boolean isClosed() {
        return delegate().isClosed();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public RestApiVersion getRestApiVersion() {
        return delegate().getRestApiVersion();
    }

    @Override
    public DeprecationHandler getDeprecationHandler() {
        return delegate().getDeprecationHandler();
    }

    // AbstractXContent do* methods are not supported.
    // All the high level XContentParser methods are reimplemented with delegation (not using do*)
    // the overriden with metering behaviour method is nextToken
    // AbstractXContent

    @Override
    protected boolean doBooleanValue() throws IOException {
        throw new UnsupportedOperationException("do variants should not be used");
    }

    @Override
    protected short doShortValue() throws IOException {
        throw new UnsupportedOperationException("do variants should not be used");
    }

    @Override
    protected int doIntValue() throws IOException {
        throw new UnsupportedOperationException("do variants should not be used");
    }

    @Override
    protected long doLongValue() throws IOException {
        throw new UnsupportedOperationException("do variants should not be used");
    }

    @Override
    protected float doFloatValue() throws IOException {
        throw new UnsupportedOperationException("do variants should not be used");
    }

    @Override
    protected double doDoubleValue() throws IOException {
        throw new UnsupportedOperationException("do variants should not be used");
    }
}
