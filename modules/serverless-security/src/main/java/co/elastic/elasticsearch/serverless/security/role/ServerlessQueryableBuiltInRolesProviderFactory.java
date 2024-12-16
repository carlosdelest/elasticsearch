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

package co.elastic.elasticsearch.serverless.security.role;

import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.security.support.QueryableBuiltInRolesProviderFactory;

public class ServerlessQueryableBuiltInRolesProviderFactory implements QueryableBuiltInRolesProviderFactory {

    public ServerlessQueryableBuiltInRolesProviderFactory() {}

    @Override
    public ServerlessQueryableBuiltInRolesProvider createProvider(ReservedRolesStore reservedRolesStore, FileRolesStore fileRolesStore) {
        return new ServerlessQueryableBuiltInRolesProvider(fileRolesStore);
    }

}
