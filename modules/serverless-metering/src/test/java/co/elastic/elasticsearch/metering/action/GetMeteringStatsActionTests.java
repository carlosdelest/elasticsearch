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

package co.elastic.elasticsearch.metering.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class GetMeteringStatsActionTests extends ESTestCase {

    public void testEmptyResponseToXContent() throws IOException {

        var response = new GetMeteringStatsAction.Response(0L, 0L, Map.of(), Map.of(), Map.of());

        var serialized = Strings.toString(ChunkedToXContent.wrapAsToXContent(response));
        assertEquals(XContentHelper.stripWhitespace("""
            {
               "_total": {
                  "num_docs": 0
               },
               "indices": [],
               "datastreams": []
            }
            """), serialized);
    }

    public void testSimpleResponseToXContent() throws IOException {

        var response = new GetMeteringStatsAction.Response(
            20L,
            200L,
            Map.ofEntries(Map.entry("index1", new GetMeteringStatsAction.MeteringStats(100L, 10L))),
            Map.of(),
            Map.of()
        );

        var serialized = Strings.toString(ChunkedToXContent.wrapAsToXContent(response));
        assertEquals(XContentHelper.stripWhitespace("""
            {
               "_total": {
                  "num_docs": 20
               },
               "indices": [
                  {
                     "name": "index1",
                     "num_docs": 10
                  }
               ],
               "datastreams": []
            }
            """), serialized);
    }

    @SuppressWarnings("unchecked")
    public void testResponseWithDataStreamToXContent() throws IOException {

        var response = new GetMeteringStatsAction.Response(
            20L,
            200L,
            Map.ofEntries(
                Map.entry("index1", new GetMeteringStatsAction.MeteringStats(100L, 10L)),
                Map.entry(".ds_index1", new GetMeteringStatsAction.MeteringStats(120L, 12L)),
                Map.entry(".ds_index1-1", new GetMeteringStatsAction.MeteringStats(80L, 8L))
            ),
            Map.ofEntries(Map.entry(".ds_index1", "datastream1"), Map.entry(".ds_index1-1", "datastream1")),
            Map.ofEntries(Map.entry("datastream1", new GetMeteringStatsAction.MeteringStats(200L, 20L)))
        );

        var serialized = Strings.toString(ChunkedToXContent.wrapAsToXContent(response));

        Map<String, Object> responseMap = entityAsMap(serialized);
        List<Object> indices = (List<Object>) responseMap.get("indices");
        assertThat(indices.size(), equalTo(3));
        Map<String, Object> index1 = findWithName(indices, "index1");
        assertThat(index1, notNullValue());

        Map<String, Object> dsIndex1 = findWithName(indices, ".ds_index1");
        assertThat(dsIndex1, notNullValue());
        assertThat(dsIndex1, hasKey("datastream"));
        assertThat(dsIndex1.get("datastream"), is("datastream1"));

        Map<String, Object> dsIndex2 = findWithName(indices, ".ds_index1-1");
        assertThat(dsIndex1, notNullValue());
        assertThat(dsIndex1, hasKey("datastream"));
        assertThat(dsIndex1.get("datastream"), is("datastream1"));

        List<Object> datastreams = (List<Object>) responseMap.get("datastreams");
        assertThat(datastreams.size(), equalTo(1));
        Map<String, Object> datastream1 = (Map<String, Object>) datastreams.get(0);
        String datastreamName1 = (String) datastream1.get("name");

        assertThat(datastreamName1, is("datastream1"));
    }

    @SuppressWarnings("unchecked")
    public void testResponseWithMultipleEntriesToXContent() throws IOException {

        var response = new GetMeteringStatsAction.Response(
            20L,
            200L,
            Map.ofEntries(
                Map.entry("index1", new GetMeteringStatsAction.MeteringStats(100L, 10L)),
                Map.entry("index2", new GetMeteringStatsAction.MeteringStats(110L, 11L))
            ),
            Map.ofEntries(
                Map.entry(".ds_index1", "datastream1"),
                Map.entry(".ds_index1-1", "datastream1"),
                Map.entry(".ds_index2", "datastream2")
            ),
            Map.ofEntries(
                Map.entry("datastream1", new GetMeteringStatsAction.MeteringStats(200L, 20L)),
                Map.entry("datastream2", new GetMeteringStatsAction.MeteringStats(200L, 20L))
            )
        );

        var serialized = Strings.toString(ChunkedToXContent.wrapAsToXContent(response));

        Map<String, Object> responseMap = entityAsMap(serialized);
        List<Object> indices = (List<Object>) responseMap.get("indices");
        assertThat(indices.size(), equalTo(2));
        Map<String, Object> index1 = (Map<String, Object>) indices.get(0);
        String indexName1 = (String) index1.get("name");
        Map<String, Object> index2 = (Map<String, Object>) indices.get(1);
        String indexName2 = (String) index2.get("name");

        assertThat(List.of(indexName1, indexName2), containsInAnyOrder("index1", "index2"));

        List<Object> datastreams = (List<Object>) responseMap.get("datastreams");
        assertThat(datastreams.size(), equalTo(2));
        Map<String, Object> datastream1 = (Map<String, Object>) datastreams.get(0);
        String datastreamName1 = (String) datastream1.get("name");
        Map<String, Object> datastream2 = (Map<String, Object>) datastreams.get(1);
        String datastreamName2 = (String) datastream2.get("name");

        assertThat(List.of(datastreamName1, datastreamName2), containsInAnyOrder("datastream1", "datastream2"));
    }

    private static Map<String, Object> entityAsMap(String json) throws IOException {
        XContentType xContentType = XContentType.JSON;
        // EMPTY and THROW are fine here because `.map` doesn't use named x content or deprecation
        try (
            XContentParser parser = xContentType.xContent()
                .createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(NamedXContentRegistry.EMPTY)
                        .withDeprecationHandler(DeprecationHandler.THROW_UNSUPPORTED_OPERATION),
                    json
                )
        ) {
            return parser.map();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> findWithName(List<Object> list, String name) {
        for (Object o : list) {
            Map<String, Object> data = (Map<String, Object>) o;
            String nameField = (String) data.get("name");
            if (name.equals(nameField)) {
                return data;
            }
        }
        return null;
    }
}
