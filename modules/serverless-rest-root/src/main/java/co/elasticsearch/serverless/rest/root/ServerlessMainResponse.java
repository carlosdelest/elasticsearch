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

package co.elasticsearch.serverless.rest.root;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ServerlessMainResponse extends ActionResponse implements ToXContentObject {

    private String nodeName;
    private ClusterName clusterName;
    private String clusterUuid;
    private Build build;

    ServerlessMainResponse(StreamInput in) throws IOException {
        super(in);
        nodeName = in.readString();
        clusterName = new ClusterName(in);
        clusterUuid = in.readString();
        build = Build.readBuild(in);

    }

    public ServerlessMainResponse(String nodeName, ClusterName clusterName, String clusterUuid, Build build) {
        this.nodeName = nodeName;
        this.clusterName = clusterName;
        this.clusterUuid = clusterUuid;
        this.build = build;
    }

    public String getNodeName() {
        return nodeName;
    }

    public ClusterName getClusterName() {
        return clusterName;
    }

    public String getClusterUuid() {
        return clusterUuid;
    }

    public Build getBuild() {
        return build;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeName);
        clusterName.writeTo(out);
        out.writeString(clusterUuid);
        Build.writeBuild(build, out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (ServerlessScope.SERVERLESS_RESTRICTION.equals(params.param(RestRequest.RESPONSE_RESTRICTED)) == false) {
            builder.field("name", nodeName);
            builder.field("cluster_name", clusterName.value());
            builder.field("cluster_uuid", clusterUuid);
        }
        builder.startObject("version").field("build_flavor", "serverless").endObject();
        builder.field("tagline", "You Know, for Search");

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServerlessMainResponse other = (ServerlessMainResponse) o;
        return Objects.equals(nodeName, other.nodeName)
            && Objects.equals(clusterUuid, other.clusterUuid)
            && Objects.equals(build, other.build)
            && Objects.equals(clusterName, other.clusterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeName, clusterUuid, build, clusterName);
    }

    @Override
    public String toString() {
        return "MainResponse{"
            + "nodeName='"
            + nodeName
            + '\''
            + ", clusterName="
            + clusterName
            + ", clusterUuid='"
            + clusterUuid
            + '\''
            + ", build="
            + build
            + '}';
    }
}
