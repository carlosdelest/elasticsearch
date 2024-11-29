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

package co.elastic.elasticsearch.serverless.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.search.suggest.document.Completion912PostingsFormat;
import org.apache.lucene.search.suggest.document.CompletionPostingsFormat;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;

import java.io.IOException;

/**
 * This postings format allows the completion FST to be loaded on-heap or off-heap, where off-heap can be used to save a great deal of
 * memory at the minor expense of performance (minor given that networking overhead dwarfs any differences).
 * <p>
 * This class was created as a workaround for current (at the time of writing) Lucene deficiencies. Lucene's
 * {@link CompletionPostingsFormat} provides a constructor to instantiate Completion FSTs off-heap, but this is unusable, as Lucene's
 * Service Loader mechanism defaults to loading FSTs on-heap. We must use Lucene's Service Loader at read-time (which loads the codec by the
 * name written to the Segment Info), and there is no way to configure the implementations that Lucene provides (e.g.
 * {@link Completion912PostingsFormat} at this point of the lifecycle.
 * <p>
 * This new codec works as follows: At node startup, this class is configured as per the setting, using a static variable. When a field is
 * created at write-time, the name of this codec is written into the Segment Info. At read-time, Lucene's Service Loader will read that name
 * to load this codec.  This Codec will then delegate to a Lucene class constructed to load the FST on-heap or off-heap, as per the
 * configuration.
 */
public class Elasticsearch900Lucene100CompletionPostingsFormat extends CompletionPostingsFormat {
    public static final Setting<Boolean> COMPLETION_FST_ON_HEAP = Setting.boolSetting(
        "serverless.lucene.codec.completion_fst_on_heap",
        true,
        Setting.Property.NodeScope
    );
    public static final String NAME = "Elasticsearch900Lucene100CompletionPostingsFormat";
    private static final Logger logger = LogManager.getLogger(Elasticsearch900Lucene100CompletionPostingsFormat.class);
    private static final String DELEGATE_POSTINGS_FORMAT_NAME = "Lucene912";
    private static final SetOnce<Boolean> fstOnHeap = new SetOnce<>();

    private final CompletionPostingsFormat offHeapDelegate = new CompletionPostingsFormatOffHeap();

    public Elasticsearch900Lucene100CompletionPostingsFormat() {
        super(NAME, FSTLoadMode.ON_HEAP);
    }

    @Override
    protected PostingsFormat delegatePostingsFormat() {
        return PostingsFormat.forName(DELEGATE_POSTINGS_FORMAT_NAME);
    }

    /**
     * Configure whether the FST should be loaded on heap or off heap.
     *
     * @param fstOnHeap true if the FST should be loaded on heap, false if it should be loaded off heap.
     */
    public static void configureFSTOnHeap(boolean fstOnHeap) {
        try {
            Elasticsearch900Lucene100CompletionPostingsFormat.fstOnHeap.set(fstOnHeap);
        } catch (SetOnce.AlreadySetException e) {
            // If you see this in an IT test, ignore it. It's a side effect of the way IT tests are run,
            // where all nodes share the same JVM and thus, the same class loader.
            logger.error(
                Strings.format(
                    "Error setting fstOnHeap to %s, already set to %s. (This is expected in IT tests, but nowhere else.)",
                    fstOnHeap,
                    Elasticsearch900Lucene100CompletionPostingsFormat.fstOnHeap.get()
                )
            );
        }
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        if (Boolean.TRUE.equals(fstOnHeap.get())) {
            logger.trace("On Heap fieldsProducer called");
            return super.fieldsProducer(state);
        } else {
            logger.trace("Off Heap fieldsProducer called");
            return offHeapDelegate.fieldsProducer(state);
        }
    }

    private static class CompletionPostingsFormatOffHeap extends CompletionPostingsFormat {
        private CompletionPostingsFormatOffHeap() {
            super(NAME, FSTLoadMode.OFF_HEAP);
        }

        @Override
        protected PostingsFormat delegatePostingsFormat() {
            return PostingsFormat.forName(DELEGATE_POSTINGS_FORMAT_NAME);
        }
    }
}
