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

package org.elasticsearch.wipe.cli;

import org.elasticsearch.cli.CliToolProvider;
import org.elasticsearch.cli.Command;

public class ServerlessWipeDataCliProvider implements CliToolProvider {
    @Override
    public String name() {
        return "serverless-wipe-data";
    }

    @Override
    public Command create() {
        return new ServerlessWipeDataCli();
    }
}
