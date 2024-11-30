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

import org.elasticsearch.jdk.ModuleQualifiedExportsService;

/**
 * This is used to allow the codec module to be exported with qualified exports.
 */
public class CodecModuleQualifiedExportsService extends ModuleQualifiedExportsService {
    final Module module = CodecModuleQualifiedExportsService.class.getModule();

    @Override
    protected void addExports(String pkg, Module target) {
        module.addExports(pkg, target);
    }

    @Override
    protected void addOpens(String pkg, Module target) {
        module.addOpens(pkg, target);
    }
}
