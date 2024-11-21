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

package co.elastic.elasticsearch.stateless.cache;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.index.store.LuceneFilesExtensions;

import java.io.IOException;
import java.util.Map;

/**
 * This file is mostly copied from org.apache.lucene.codecs.lucene90.Lucene90CompoundReader
 * in order to be able to parse compound segment entries in order to prewarm them.
 * Currently, it is impossible to reuse the original class as the necessary code has private access
 */
public class Lucene90CompoundEntriesReader {

    static final String ENTRY_CODEC = "Lucene90CompoundEntries";
    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;

    public static Map<String, FileEntry> readEntries(Directory directory, String filename) throws IOException {
        assert LuceneFilesExtensions.fromFile(filename) == LuceneFilesExtensions.CFE : filename;
        try (var input = directory.openInput(filename, IOContext.READONCE)) {
            return Lucene90CompoundEntriesReader.readEntries(input);
        }
    }

    /**
     * This method skips the input validation and only lists the entries in a cfe file.
     * Validation is going to be performed later once directory is opened for the index engine.
     */
    public static Map<String, FileEntry> readEntries(DataInput dataInput) throws IOException {
        CodecUtil.checkHeader(dataInput, ENTRY_CODEC, VERSION_START, VERSION_CURRENT);
        dataInput.skipBytes(StringHelper.ID_LENGTH);
        CodecUtil.checkIndexHeaderSuffix(dataInput, "");
        return readMapping(dataInput);
    }

    private static Map<String, FileEntry> readMapping(DataInput entriesStream) throws IOException {
        final int numEntries = entriesStream.readVInt();
        var mapping = CollectionUtil.<String, FileEntry>newHashMap(numEntries);
        for (int i = 0; i < numEntries; i++) {
            final String id = entriesStream.readString();
            final FileEntry fileEntry = new FileEntry(entriesStream.readLong(), entriesStream.readLong());
            FileEntry previous = mapping.put(id, fileEntry);
            if (previous != null) {
                throw new CorruptIndexException("Duplicate cfs entry id=" + id + " in CFS ", entriesStream);
            }
        }
        return mapping;
    }

    public record FileEntry(long offset, long length) {}
}
