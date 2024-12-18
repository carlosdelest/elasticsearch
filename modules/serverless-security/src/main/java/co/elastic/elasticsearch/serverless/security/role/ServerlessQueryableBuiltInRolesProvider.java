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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.security.support.QueryableBuiltInRoles;
import org.elasticsearch.xpack.security.support.QueryableBuiltInRoles.Listener;
import org.elasticsearch.xpack.security.support.QueryableBuiltInRolesUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ServerlessQueryableBuiltInRolesProvider implements QueryableBuiltInRoles.Provider {

    private static final Logger logger = LogManager.getLogger(ServerlessQueryableBuiltInRolesProvider.class);

    private final AtomicReference<QueryableBuiltInRoles> currentRoles = new AtomicReference<>();
    private final List<Listener> listeners = new ArrayList<>();
    private final Supplier<QueryableBuiltInRoles> rolesSupplier;
    private final FileRolesStore fileRolesStore;

    public ServerlessQueryableBuiltInRolesProvider(FileRolesStore fileRolesStore) {
        this.fileRolesStore = fileRolesStore;
        this.rolesSupplier = this::fileBuiltInRoles;
        this.fileRolesStore.addListener(this::onFileRolesChanged);
    }

    @Override
    public QueryableBuiltInRoles getRoles() {
        return currentRoles.updateAndGet(roles -> {
            if (roles == null) {
                return rolesSupplier.get();
            }
            return roles;
        });
    }

    @Override
    public void addListener(Listener listener) {
        synchronized (this) {
            listeners.add(listener);
        }
    }

    private void onFileRolesChanged(Set<String> roleNames) {
        logger.trace("File roles changed, updating queryable built-in roles and notifying listeners");
        final QueryableBuiltInRoles oldRoles = currentRoles.get();
        final QueryableBuiltInRoles newRoles = rolesSupplier.get();
        if (currentRoles.compareAndSet(oldRoles, newRoles)) {
            notifyListeners(newRoles);
        }
    }

    private void notifyListeners(QueryableBuiltInRoles changedRoles) {
        logger.trace("notifying listeners of built-in roles change");
        synchronized (this) {
            for (final Listener listener : listeners) {
                try {
                    listener.onRolesChanged(changedRoles);
                } catch (Exception e) {
                    logger.warn("Failed to notify listener of queryable role changes", e);
                }
            }
        }
    }

    private QueryableBuiltInRoles fileBuiltInRoles() {
        final Map<String, RoleDescriptor> allRoles = fileRolesStore.getAllRoleDescriptors();
        final Map<String, String> rolesDigests = new HashMap<>();
        final Set<RoleDescriptor> builtInRoles = new HashSet<>();
        for (final RoleDescriptor role : allRoles.values()) {
            if (ServerlessRoleValidator.isMarkedPublic(role)) {
                builtInRoles.add(role);
                rolesDigests.put(role.getName(), QueryableBuiltInRolesUtils.calculateHash(role));
            }
        }
        return new QueryableBuiltInRoles(Collections.unmodifiableMap(rolesDigests), Collections.unmodifiableSet(builtInRoles));
    }

}
