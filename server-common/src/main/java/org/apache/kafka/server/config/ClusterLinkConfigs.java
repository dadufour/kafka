/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class ClusterLinkConfigs {

    public enum LinkMode {
        Active("ACTIVE"),
        Standby("STANDBY");

        private final String linkMode;

        LinkMode(String linkMode) {
            this.linkMode = linkMode;
        }

        @Override
        public String toString() {
            return linkMode;
        }
    }

    public static final String CLUSTER_LINK_PREFIX = "cluster.link.";

    public static final String CLUSTER_LINK_MODE_CONFIG = CLUSTER_LINK_PREFIX + "mode";
    public static final String CLUSTER_LINK_MODE_DEFAULT = LinkMode.Active.toString();
    public static final String CLUSTER_LINK_MODE_DOC = "Active/Standby mode - can be one of ACTIVE | STANDBY";

    public static final String CLUSTER_LINK_SOURCE_CLUSTER_ID_CONFIG = CLUSTER_LINK_PREFIX + "source.cluster.id";
    public static final String CLUSTER_LINK_SOURCE_CLUSTER_ID_DEFAULT = "";
    public static final String CLUSTER_LINK_SOURCE_CLUSTER_ID_DOC = "Cluster Id of the source cluster";

    public static final String CLUSTER_LINK_SOURCE_QUORUM_BOOTSTRAP_SERVERS_CONFIG = CLUSTER_LINK_PREFIX + "source.quorum.bootstrap.servers";
    public static final List<String> CLUSTER_LINK_SOURCE_QUORUM_BOOTSTRAP_SERVERS_DEFAULT = List.of();
    public static final String CLUSTER_LINK_SOURCE_QUORUM_BOOTSTRAP_SERVERS_DOC = "Bootstrap servers of the source quorum cluster";

    public static final String CLUSTER_LINK_LISTENER_NAMES_CONFIG = CLUSTER_LINK_PREFIX + "listener.names";
    public static final List<String> CLUSTER_LINK_LISTENER_NAMES_DEFAULT = List.of();
    public static final String CLUSTER_LINK_LISTENER_NAMES_DOC = "A comma-separated list of the names of the listeners used by the link observer. This is required " +
            "when the cluster is used as Cluster Link Standby.";

    public static final String CLUSTER_LINK_OBSERVER_NODE_ID_CONFIG = CLUSTER_LINK_PREFIX + "observer.node.id";
    public static final int CLUSTER_LINK_OBSERVER_NODE_ID_DEFAULT = 5000; 
    public static final String CLUSTER_LINK_OBSERVER_NODE_ID_DOC = "Node id of this node when observing the source quorum cluster";

    public static final ConfigDef CONFIG_DEF =  new ConfigDef()
            .define(CLUSTER_LINK_MODE_CONFIG, STRING, CLUSTER_LINK_MODE_DEFAULT, HIGH, CLUSTER_LINK_MODE_DOC)
            .define(CLUSTER_LINK_SOURCE_CLUSTER_ID_CONFIG, STRING, CLUSTER_LINK_SOURCE_CLUSTER_ID_DEFAULT, HIGH, CLUSTER_LINK_SOURCE_CLUSTER_ID_DOC)
            .define(CLUSTER_LINK_SOURCE_QUORUM_BOOTSTRAP_SERVERS_CONFIG, LIST, CLUSTER_LINK_SOURCE_QUORUM_BOOTSTRAP_SERVERS_DEFAULT, new ClusterLinkBootstrapServersValidator(), HIGH, CLUSTER_LINK_SOURCE_QUORUM_BOOTSTRAP_SERVERS_DOC)
            .define(CLUSTER_LINK_LISTENER_NAMES_CONFIG, LIST, CLUSTER_LINK_LISTENER_NAMES_DEFAULT, ConfigDef.ValidList.anyNonDuplicateValues(true, false), HIGH, CLUSTER_LINK_LISTENER_NAMES_DOC)
            .define(CLUSTER_LINK_OBSERVER_NODE_ID_CONFIG, INT, CLUSTER_LINK_OBSERVER_NODE_ID_DEFAULT, atLeast(1000), HIGH, CLUSTER_LINK_OBSERVER_NODE_ID_DOC);

    private final String mode;
    private final String sourceClusterId;
    private final List<String> sourceQuorumBootstrapServers;
    private final List<String> listenerNames;
    private final int observerNodeId;

    public ClusterLinkConfigs(AbstractConfig abstractConfig) {
        this.mode = abstractConfig.getString(CLUSTER_LINK_MODE_CONFIG);
        this.sourceClusterId = abstractConfig.getString(CLUSTER_LINK_SOURCE_CLUSTER_ID_CONFIG);
        this.sourceQuorumBootstrapServers = abstractConfig.getList(CLUSTER_LINK_SOURCE_QUORUM_BOOTSTRAP_SERVERS_CONFIG);
        this.listenerNames = abstractConfig.getList(CLUSTER_LINK_LISTENER_NAMES_CONFIG);
        this.observerNodeId = abstractConfig.getInt(CLUSTER_LINK_OBSERVER_NODE_ID_CONFIG);
    }
   
    public String mode() {
        return mode;
    }

    public String sourceClusterId() {
        return sourceClusterId;
    }

    public List<String> sourceQuorumBootstrapServers() {
        return sourceQuorumBootstrapServers;
    }

    public List<String> listenerNames() {
        return listenerNames;
    }

    public int observerNodeId() {
        return observerNodeId;
    }
    
    public static List<InetSocketAddress> parseBootstrapServers(List<String> bootstrapServers) {
        return bootstrapServers
            .stream()
            .map(ClusterLinkConfigs::parseBootstrapServer)
            .collect(Collectors.toList());
    }

    private static InetSocketAddress parseBootstrapServer(String bootstrapServer) {
        String host = Utils.getHost(bootstrapServer);
        if (host == null || !Utils.validHostPattern(host)) {
            throw new ConfigException(
                String.format(
                    "Failed to parse host name from %s for the configuration %s. Each " +
                    "entry should be in the form \"{host}:{port}\"",
                    bootstrapServer,
                    CLUSTER_LINK_SOURCE_QUORUM_BOOTSTRAP_SERVERS_CONFIG
                )
            );
        }

        Integer port = Utils.getPort(bootstrapServer);
        if (port == null) {
            throw new ConfigException(
                String.format(
                    "Failed to parse host port from %s for the configuration %s. Each " +
                    "entry should be in the form \"{host}:{port}\"",
                    bootstrapServer,
                    CLUSTER_LINK_SOURCE_QUORUM_BOOTSTRAP_SERVERS_CONFIG
                )
            );
        }

        return InetSocketAddress.createUnresolved(host, port);
    }

    public static class ClusterLinkBootstrapServersValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            if (value == null) {
                throw new ConfigException(name, null);
            }

            @SuppressWarnings("unchecked")
            List<String> entries = (List<String>) value;

            // Attempt to parse the connect strings
            for (String entry : entries) {
                parseBootstrapServer(entry);
            }
        }

        @Override
        public String toString() {
            return "non-empty list";
        }
    }
}
