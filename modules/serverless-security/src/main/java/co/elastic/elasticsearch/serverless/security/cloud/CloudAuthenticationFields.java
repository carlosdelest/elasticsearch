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

package co.elastic.elasticsearch.serverless.security.cloud;

public final class CloudAuthenticationFields {

    /**
     * The key used to store the project information of the project that is being authenticated against.
     */
    public static final String AUTHENTICATING_PROJECT_METADATA_KEY = "_security_serverless_authenticating_project";

    public static final String PROJECT_ID_FIELD = "project_id";
    public static final String PROJECT_ORGANIZATION_FIELD = "project_organization_id";
    public static final String PROJECT_TYPE_FIELD = "project_type";
    public static final String APPLICATION_ROLES_FIELD = "application_roles";
    public static final String CONTEXTS_FIELD = "contexts";
    public static final String TYPE_FIELD = "type";

}
