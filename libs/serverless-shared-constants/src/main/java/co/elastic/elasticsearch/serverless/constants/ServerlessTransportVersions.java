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

package co.elastic.elasticsearch.serverless.constants;

import org.elasticsearch.TransportVersion;

import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import static org.elasticsearch.TransportVersions.getAllVersionIds;

/**
 * A container for TransportVersion constants used by the serverless repo.
 */
public class ServerlessTransportVersions {

    static Set<Integer> IDS = new HashSet<>();

    public static final TransportVersion DUMMY_PRIMARY_RELOCATION_CHANGE = def(8_515_01_0);
    public static final TransportVersion DUMMY_PRIMARY_RELOCATION_CHANGE_FIX = def(8_517_01_0);
    public static final TransportVersion STALE_INDICES_GC = def(8_531_01_0);
    public static final TransportVersion COMPOUND_COMMIT_WITH_SIZE = def(8_594_01_0);
    public static final TransportVersion INDEX_SIZE_SERVICE_ADDED = def(8_594_02_0);
    public static final TransportVersion NEW_COMMIT_NOTIFICATION_WITH_BCC_INFO = def(8_612_01_0);
    public static final TransportVersion METERING_SHARD_INFO_DOC_COUNT_ADDED = def(8_612_02_0);
    public static final TransportVersion SEARCH_LOAD_AUTOSCALING = def(8_618_01_0);
    public static final TransportVersion BLOB_LOCATION_WITHOUT_BLOB_LENGTH = def(8_619_01_0);
    /*
     * STOP! READ THIS FIRST! No, really,
     *        ____ _____ ___  ____  _        ____  _____    _    ____    _____ _   _ ___ ____    _____ ___ ____  ____ _____ _
     *       / ___|_   _/ _ \|  _ \| |      |  _ \| ____|  / \  |  _ \  |_   _| | | |_ _/ ___|  |  ___|_ _|  _ \/ ___|_   _| |
     *       \___ \ | || | | | |_) | |      | |_) |  _|   / _ \ | | | |   | | | |_| || |\___ \  | |_   | || |_) \___ \ | | | |
     *        ___) || || |_| |  __/|_|      |  _ <| |___ / ___ \| |_| |   | | |  _  || | ___) | |  _|  | ||  _ < ___) || | |_|
     *       |____/ |_| \___/|_|   (_)      |_| \_\_____/_/   \_\____/    |_| |_| |_|___|____/  |_|   |___|_| \_\____/ |_| (_)
     *
     * A new transport version should be added EVERY TIME a change is made to the serialization protocol of one or more classes. Each
     * transport version should only be used in a single merged commit.
     *
     * To add a new transport version for use in serverless code, add a new constant at the bottom of the list, above this comment.
     *
     * See TransportVersions.java for a description of the transport version id layout.
     *
     * The new version constant should be based on a version constant from the elasticsearch repo. Take the id of the latest version
     * constant defined in TransportVersions. There are two possible cases:
     * - If the server part of the id is equal to the server part from the latest constant defined here, then
     *   bump the server part of the latest version here. eg if the latest id defined in server is 8_501_00_0, and the latest defined
     *   here is 8_501_01_0, then the next id should be 8_501_02_0.
     * - If the server part of the id is newer than the server part from the latest constant defined here, then the next id
     *   should bump the server part of that id. eg if the latest id defined in server is 8_600_00_0, and the latest defined
     *   here is 8_500_01_0, then the next id should be 8_600_01_0.
     *
     * A patch id should only be created if a patch transport change is needed in serverless code. If so, the next id should be
     * the latest defined here with the patch increment. eg if the latest id defined here is 8_500_01_0, then the next patch
     * id should be 8_500_01_1.
     */

    static TransportVersion def(int id) {
        if (IDS == null) throw new IllegalStateException("The IDS map needs to be present to call this method");

        if (IDS.add(id) == false) {
            throw new IllegalArgumentException("Version id " + id + " defined twice");
        }
        return new TransportVersion(id);
    }

    static final NavigableMap<Integer, TransportVersion> VERSION_IDS = getAllVersionIds(ServerlessTransportVersions.class);

    // the highest transport version constant defined in this file
    static final TransportVersion LATEST_DEFINED;
    static {
        LATEST_DEFINED = VERSION_IDS.lastEntry().getValue();

        // see comment on IDS field
        // now we're registered all the transport versions, we can clear the map
        IDS = null;
    }

    // no instance
    private ServerlessTransportVersions() {}
}
