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

package co.elastic.elasticsearch.metering.codec;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static co.elastic.elasticsearch.metering.reporter.RAStorageAccumulator.RA_STORAGE_AVG_KEY;
import static co.elastic.elasticsearch.metering.reporter.RAStorageAccumulator.RA_STORAGE_KEY;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class RAStorageDocValuesConsumerTests extends BaseDocValuesFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
    }

    @Override
    protected Codec getCodec() {
        var parentCodecDocValuesFormat = new ES87TSDBDocValuesFormat();
        var extensionDocValuesFormat = new RAStorageDocValuesFormatFactory().createDocValueFormat(parentCodecDocValuesFormat);
        return TestUtil.alwaysDocValuesFormat(extensionDocValuesFormat);
    }

    public void testAttributeWrittenWithAverageValue() throws IOException {
        Directory directory = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random());
        IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
        iwconfig.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

        Document doc = new Document();
        doc.add(new StringField("id", "0", Field.Store.NO));
        doc.add(new NumericDocValuesField(RA_STORAGE_KEY, 11L));
        iwriter.addDocument(doc);

        doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.NO));
        doc.add(new NumericDocValuesField(RA_STORAGE_KEY, 13L));
        iwriter.addDocument(doc);

        // ES-9361: documents containing a negative RA-S will be ignored
        doc = new Document();
        doc.add(new StringField("id", "2", Field.Store.NO));
        doc.add(new NumericDocValuesField(RA_STORAGE_KEY, -1));
        iwriter.addDocument(doc);

        iwriter.commit();

        DirectoryReader ireader = iwriter.getReader();
        iwriter.close();

        var segmentInfos = Lucene.readSegmentInfos(directory);
        var attribute = segmentInfos.info(0).info.getAttribute(RA_STORAGE_AVG_KEY);
        assertNotNull(attribute);

        assertEquals(12L, Long.parseLong(attribute));

        List<Long> values = new ArrayList<>();
        NumericDocValues dv = getOnlyLeafReader(ireader).getNumericDocValues(RA_STORAGE_KEY);
        assertNotEquals(NO_MORE_DOCS, dv.nextDoc());
        values.add(dv.longValue());
        assertNotEquals(NO_MORE_DOCS, dv.nextDoc());
        values.add(dv.longValue());
        assertNotEquals(NO_MORE_DOCS, dv.nextDoc());
        values.add(dv.longValue());
        assertEquals(NO_MORE_DOCS, dv.nextDoc());

        var expected = List.of(11L, 13L, -1L);
        assertTrue(values.containsAll(expected));

        ireader.close();
        directory.close();
    }

    /**
     * Test that the _rastorage_avg attribute is written when we are in a mixed segment (some docs have the field, some do not have it)
     */
    public void testAttributeWrittenWithAverageValueInMixedSegment() throws IOException {
        Directory directory = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random());
        IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
        iwconfig.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

        Document doc = new Document();
        doc.add(new StringField("id", "0", Field.Store.NO));
        iwriter.addDocument(doc);

        doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.NO));
        doc.add(new NumericDocValuesField(RA_STORAGE_KEY, 13L));
        iwriter.addDocument(doc);

        iwriter.commit();

        DirectoryReader ireader = iwriter.getReader();
        iwriter.close();

        var segmentInfos = Lucene.readSegmentInfos(directory);
        var attribute = segmentInfos.info(0).info.getAttribute(RA_STORAGE_AVG_KEY);
        assertNotNull(attribute);

        assertEquals(13L, Long.parseLong(attribute));

        List<Long> values = new ArrayList<>();
        NumericDocValues dv = getOnlyLeafReader(ireader).getNumericDocValues(RA_STORAGE_KEY);
        assertNotEquals(NO_MORE_DOCS, dv.nextDoc());
        values.add(dv.longValue());
        assertEquals(NO_MORE_DOCS, dv.nextDoc());

        var expected = List.of(13L);
        assertTrue(values.containsAll(expected));

        ireader.close();
        directory.close();
    }

    /**
     * Test that the _rastorage_avg attribute is updated after a delete when we are in a mixed segment
     * (some docs have the field, some do not have it)
     */
    public void testAttributeUpdatedAfterDeleteWithMergeInMixedSegment() throws IOException {
        Directory directory = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random());
        IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
        iwconfig.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

        Document doc = new Document();
        doc.add(new StringField("id", "0", Field.Store.NO));
        iwriter.addDocument(doc);

        doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.NO));
        doc.add(new NumericDocValuesField(RA_STORAGE_KEY, 13L));
        iwriter.addDocument(doc);

        iwriter.commit();
        iwriter.deleteDocuments(new Term("id", "1"));
        iwriter.forceMerge(1);

        DirectoryReader ireader = iwriter.getReader();
        iwriter.close();

        var segmentInfos = Lucene.readSegmentInfos(directory);
        var attribute = segmentInfos.info(0).info.getAttribute(RA_STORAGE_AVG_KEY);
        assertNotNull(attribute);

        assertEquals(0L, Long.parseLong(attribute));

        NumericDocValues dv = getOnlyLeafReader(ireader).getNumericDocValues(RA_STORAGE_KEY);
        assertEquals(NO_MORE_DOCS, dv.nextDoc());

        ireader.close();
        directory.close();
    }

    public void testAttributeUpdatedAfterDeleteWithMerge() throws IOException {
        Directory directory = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random());
        IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
        iwconfig.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

        Document doc = new Document();
        doc.add(new StringField("id", "0", Field.Store.NO));
        doc.add(new NumericDocValuesField(RA_STORAGE_KEY, 11L));
        iwriter.addDocument(doc);

        doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.NO));
        doc.add(new NumericDocValuesField(RA_STORAGE_KEY, 12L));
        iwriter.addDocument(doc);

        iwriter.commit();
        iwriter.deleteDocuments(new Term("id", "1"));
        iwriter.forceMerge(1);

        DirectoryReader ireader = iwriter.getReader();
        iwriter.close();

        var segmentInfos = Lucene.readSegmentInfos(directory);
        var attribute = segmentInfos.info(0).info.getAttribute(RA_STORAGE_AVG_KEY);
        assertNotNull(attribute);

        assertEquals(11L, Long.parseLong(attribute));

        NumericDocValues dv = getOnlyLeafReader(ireader).getNumericDocValues(RA_STORAGE_KEY);
        assertNotEquals(NO_MORE_DOCS, dv.nextDoc());
        assertEquals(11L, dv.longValue());
        assertEquals(NO_MORE_DOCS, dv.nextDoc());

        ireader.close();
        directory.close();
    }

    public void testAttributeNotPresentAfterDeleteAllWithMerge() throws IOException {
        Directory directory = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random());
        IndexWriterConfig iwconfig = newIndexWriterConfig(analyzer);
        iwconfig.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iwriter = new RandomIndexWriter(random(), directory, iwconfig);

        Document doc = new Document();
        doc.add(new StringField("id", "0", Field.Store.NO));
        doc.add(new NumericDocValuesField(RA_STORAGE_KEY, 11L));
        iwriter.addDocument(doc);

        doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.NO));
        doc.add(new NumericDocValuesField(RA_STORAGE_KEY, 12L));
        iwriter.addDocument(doc);

        iwriter.commit();
        iwriter.deleteDocuments(new Term("id", "0"));
        iwriter.deleteDocuments(new Term("id", "1"));
        iwriter.forceMerge(1);

        DirectoryReader ireader = iwriter.getReader();
        iwriter.close();

        var segmentInfos = Lucene.readSegmentInfos(directory);
        assertTrue(segmentInfos.asList().isEmpty());

        List<LeafReaderContext> subReaders = ireader.leaves();
        assertTrue(subReaders.isEmpty());

        ireader.close();
        directory.close();
    }
}
