#!/bin/bash

echo "*** importing cosmosdb emulator CA certificate into JVM cacerts ***"

cp "$JAVA_HOME/lib/security/cacerts" /opt/jboss/container/java/modified-cacerts

keytool -importcert -alias cosmosdb-root \
        -file /etc/certs/cosmosdb-root-ca.crt \
        -keystore  /opt/jboss/container/java/modified-cacerts \
        -storepass changeit -noprompt -trustcacerts


export JAVA_TOOL_OPTIONS="\
  -Djavax.net.ssl.trustStore=/opt/jboss/container/java/modified-cacerts \
  -Djavax.net.ssl.trustStorePassword=changeit \
  -Djavax.net.ssl.trustStoreType=JKS \
  -Djavax.net.debug=ssl"

chmod 777     /opt/jboss/container/java/modified-cacerts

exec /opt/jboss/container/java/run/run-java.sh

