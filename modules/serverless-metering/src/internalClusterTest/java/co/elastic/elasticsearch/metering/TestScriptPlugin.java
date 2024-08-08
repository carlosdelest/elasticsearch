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

import org.elasticsearch.script.MockScriptPlugin;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TestScriptPlugin extends MockScriptPlugin {

    @Override
    @SuppressWarnings("unchecked")
    protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        scripts.put("ctx._source.b = 'xx'", vars -> srcScript(vars, source -> source.replace("b", "xx")));
        scripts.put("ctx._source.b = 'xxx'", vars -> srcScript(vars, source -> source.replace("b", "xxx")));
        scripts.put("ctx._source.b = '0123456789'", vars -> srcScript(vars, source -> source.replace("b", "0123456789")));
        return scripts;
    }

    @SuppressWarnings("unchecked")
    static Object srcScript(Map<String, Object> vars, Function<Map<String, Object>, Object> f) {
        Map<?, ?> ctx = (Map<?, ?>) vars.get("ctx");

        Map<String, Object> source = (Map<String, Object>) ctx.get("_source");
        return f.apply(source);
    }

}
