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

package co.elastic.elasticsearch.ml.serverless;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.ml.MachineLearningExtension;

public class MachineLearningServerless extends Plugin implements MachineLearningExtension {

    private static final Logger logger = LogManager.getLogger(MachineLearningServerless.class);

    public static final String NAME = "ml-serverless";

    @Override
    public boolean useIlm() {
        logger.debug("{}::useIlm returning false.", NAME);
        return false;
    }

    @Override
    public boolean includeNodeInfo() {
        logger.debug("{}::includeNodeInfo returning false.", NAME);
        return false;
    }
}
