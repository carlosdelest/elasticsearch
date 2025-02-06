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

package org.elasticsearch.server.cli;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.LocallyMountedSecrets;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;

import java.util.Optional;

import static org.elasticsearch.common.settings.LocallyMountedSecrets.SECRETS_DIRECTORY;
import static org.elasticsearch.common.settings.LocallyMountedSecrets.SECRETS_FILE_NAME;

/**
 * Secure settings loader which loads an implementation of {@link SecureSettings} that
 * load secrets from locally mounted folder.
 */
public class LocallyMountedSecretsLoader implements SecureSettingsLoader {

    @Override
    public LoadedSecrets load(Environment environment, Terminal terminal) {
        terminal.println(
            "Using locally mounted secrets from ["
                + environment.configDir().toAbsolutePath().resolve(SECRETS_DIRECTORY).resolve(SECRETS_FILE_NAME)
                + "]"
        );
        return new LoadedSecrets(new LocallyMountedSecrets(environment), Optional.empty());
    }

    @Override
    public SecureSettings bootstrap(Environment environment, SecureString password) {
        throw new IllegalArgumentException("Bootstrapping locally mounted secrets is not supported");
    }

    @Override
    public boolean supportsSecurityAutoConfiguration() {
        return false;
    }
}
