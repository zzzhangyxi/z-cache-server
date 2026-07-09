/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zhang.cache.core.hash;

import com.zhang.cache.core.exception.NoAvailableNodeException;
import com.zhang.cache.core.constant.HashRingConstants;
import com.zhang.cache.core.metadata.cachenode.CacheNodeMetadataManager;
import com.zhang.cache.core.metadata.cachenode.CacheNodeMigrationMetadataManager;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMetadata;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMigrationMetadata;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMigrationStatus;
import com.zhang.cache.core.metadata.hotkey.HotKeyReplicationMetadataManager;
import com.zhang.cache.core.metadata.hotkey.HotKeyReplicationStatus;
import com.zhang.cache.core.metadata.hotkey.entity.HotKeyReplicationMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
@Slf4j
@Service
public class HashRouter {
    @Autowired
    private HashRingManager hashRingManager;
    @Autowired
    private HotKeyReplicationMetadataManager hotKeyReplicationMetadataManager;
    @Autowired
    private CacheNodeMetadataManager cacheNodeMetadataManager;
    @Autowired
    private CacheNodeMigrationMetadataManager cacheNodeMigrationMetadataManager;

    public CacheNodeMetadata route(String key) {
        CacheNodeMetadata migrationNode = routeMigratingKeyForRead(key);
        if (migrationNode != null) {
            return migrationNode;
        }
        HotKeyReplicationMetadata replication = getHotKeyReplicationMetadata(key);
        if (isHotKey(replication)) {
            return hotKeyRoute(key, replication);
        }
        return basicRoute(key);
    }

    public List<CacheNodeMetadata> writeRoute(String key) {
        List<CacheNodeMetadata> migrationNodes = routeMigratingKeyForWrite(key);
        if (CollectionUtils.isNotEmpty(migrationNodes)) {
            return migrationNodes;
        }
        List<CacheNodeMetadata> result = new ArrayList<>();
        result.add(basicRoute(key));
        return result;
    }

    public CacheNodeMetadata basicRoute(String key) {
        CacheNodeMetadata migrationNode = routeMigratingKeyForRead(key);
        if (migrationNode != null) {
            return migrationNode;
        }
        return routeOnHashRing(key, hashRingManager.getHashRing());
    }

    private CacheNodeMetadata routeOnHashRing(String key, NavigableMap<Long, CacheNodeMetadata> hashRing) {
        long hash = HashUtils.hash(key);
        if (MapUtils.isEmpty(hashRing)) {
            log.error("Hash ring is empty, there is no available cache node.");
            throw new NoAvailableNodeException("Hash ring is empty, there is no available cache node.");
        }

        Map.Entry<Long, CacheNodeMetadata> targetEntry = hashRing.ceilingEntry(hash);
        if (targetEntry == null) {
            targetEntry = hashRing.firstEntry();
        }
        return targetEntry.getValue();
    }

    private CacheNodeMetadata routeMigratingKeyForRead(String key) {
        MigrationRouteContext migrationRouteContext = getMigrationRouteContext(key);
        if (migrationRouteContext == null) {
            return null;
        }

        CacheNodeMigrationStatus status = migrationRouteContext.migrationMetadata.getStatus();
        if (CacheNodeMigrationStatus.DUAL_WRITE_READ_OLD.equals(status)) {
            return migrationRouteContext.oldOwner;
        }
        return migrationRouteContext.newNode;
    }

    private List<CacheNodeMetadata> routeMigratingKeyForWrite(String key) {
        MigrationRouteContext migrationRouteContext = getMigrationRouteContext(key);
        if (migrationRouteContext == null) {
            return null;
        }

        List<CacheNodeMetadata> writeNodes = new ArrayList<>();
        CacheNodeMigrationStatus status = migrationRouteContext.migrationMetadata.getStatus();
        if (CacheNodeMigrationStatus.READ_NEW_ONLY.equals(status)) {
            writeNodes.add(migrationRouteContext.newNode);
            return writeNodes;
        }

        writeNodes.add(migrationRouteContext.oldOwner);
        if (!writeNodes.contains(migrationRouteContext.newNode)) {
            writeNodes.add(migrationRouteContext.newNode);
        }
        return writeNodes;
    }

    private MigrationRouteContext getMigrationRouteContext(String key) {
        Map<String, CacheNodeMigrationMetadata> migrationMetadataMap =
                cacheNodeMigrationMetadataManager.getMigrationMetadata();
        if (MapUtils.isEmpty(migrationMetadataMap)) {
            return null;
        }

        for (CacheNodeMigrationMetadata migrationMetadata : migrationMetadataMap.values()) {
            if (migrationMetadata == null || migrationMetadata.getNewNode() == null
                    || migrationMetadata.getStatus() == null) {
                continue;
            }
            CacheNodeMetadata newNode = migrationMetadata.getNewNode();
            NavigableMap<Long, CacheNodeMetadata> oldHashRing = buildMigrationOldHashRing(newNode);
            if (MapUtils.isEmpty(oldHashRing)) {
                continue;
            }
            NavigableMap<Long, CacheNodeMetadata> newHashRing = new TreeMap<>(oldHashRing);
            addNodeToHashRing(newHashRing, newNode);

            CacheNodeMetadata oldOwner = routeOnHashRing(key, oldHashRing);
            CacheNodeMetadata newOwner = routeOnHashRing(key, newHashRing);
            if (newOwner != null && oldOwner != null && newNode.getId().equals(newOwner.getId())) {
                return new MigrationRouteContext(migrationMetadata, oldOwner, newNode);
            }
        }
        return null;
    }

    private NavigableMap<Long, CacheNodeMetadata> buildMigrationOldHashRing(CacheNodeMetadata newNode) {
        NavigableMap<Long, CacheNodeMetadata> hashRing = new TreeMap<>();
        Map<String, CacheNodeMetadata> onlineNodes = cacheNodeMetadataManager.getOnlineNodes();
        if (MapUtils.isEmpty(onlineNodes)) {
            return hashRing;
        }

        for (CacheNodeMetadata node : onlineNodes.values()) {
            if (node == null || newNode.getId().equals(node.getId())) {
                continue;
            }
            addNodeToHashRing(hashRing, node);
        }
        return hashRing;
    }

    private void addNodeToHashRing(NavigableMap<Long, CacheNodeMetadata> hashRing, CacheNodeMetadata node) {
        for (int i = 0; i < HashRingConstants.VIRTUAL_NODE_COUNT; i++) {
            String virtualNodeId = node.getId() + "#VNODE-" + i;
            Long virtualNodeHashValue = HashUtils.hash(virtualNodeId);
            hashRing.putIfAbsent(virtualNodeHashValue, node);
        }
    }

    private CacheNodeMetadata hotKeyRoute(String key, HotKeyReplicationMetadata replication) {
        List<CacheNodeMetadata> candidateNodes = new ArrayList<>();
        Map<String, CacheNodeMetadata> onlineNodes = cacheNodeMetadataManager.getOnlineNodes();

        CacheNodeMetadata primaryNode = basicRoute(key);
        if (onlineNodes.containsKey(primaryNode.getId())) {
            candidateNodes.add(primaryNode);
        }

        for (CacheNodeMetadata replicationNode : replication.getReplicationNodes()) {
            if (replicationNode == null || !onlineNodes.containsKey(replicationNode.getId())) {
                continue;
            }
            CacheNodeMetadata onlineNode = onlineNodes.get(replicationNode.getId());
            if (!candidateNodes.contains(onlineNode)) {
                candidateNodes.add(onlineNode);
            }
        }

        if (CollectionUtils.isEmpty(candidateNodes)) {
            return basicRoute(key);
        }

        return candidateNodes.get(ThreadLocalRandom.current().nextInt(candidateNodes.size()));
    }

    private boolean isHotKey(HotKeyReplicationMetadata replication) {
        return replication != null
                && isReady(replication)
                && CollectionUtils.isNotEmpty(replication.getReplicationNodes());
    }

    private boolean isReady(HotKeyReplicationMetadata replication) {
        return replication.getStatus() == null || HotKeyReplicationStatus.READY.equals(replication.getStatus());
    }

    private HotKeyReplicationMetadata getHotKeyReplicationMetadata(String key) {
        Map<String, HotKeyReplicationMetadata> replicationMetadata = hotKeyReplicationMetadataManager.getReplicationMetadata();
        if (MapUtils.isEmpty(replicationMetadata)) {
            return null;
        }
        return replicationMetadata.get(key);
    }

    private static class MigrationRouteContext {
        private final CacheNodeMigrationMetadata migrationMetadata;
        private final CacheNodeMetadata oldOwner;
        private final CacheNodeMetadata newNode;

        private MigrationRouteContext(
                CacheNodeMigrationMetadata migrationMetadata,
                CacheNodeMetadata oldOwner,
                CacheNodeMetadata newNode) {
            this.migrationMetadata = migrationMetadata;
            this.oldOwner = oldOwner;
            this.newNode = newNode;
        }
    }
}
