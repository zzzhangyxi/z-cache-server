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
package com.zhang.cache.controlplane.service;

import com.zhang.cache.core.constant.HashRingConstants;
import com.zhang.cache.core.hash.HashUtils;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMetadata;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMigrationMetadata;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMigrationStatus;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeRuntimeMetadata;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeStatus;
import com.zhang.cache.core.repository.BusinessRepository;
import com.zhang.cache.core.repository.MetadataRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
@Service
@Slf4j
public class MetadataRegisterService {
    private static final long MIGRATION_METADATA_PROPAGATION_DELAY_MILLIS = 5000L;

    @Autowired
    private MetadataRepository metadataRepository;
    @Autowired
    private BusinessRepository businessRepository;

    public void register(String id, String ip, Integer port) {
        long now = System.currentTimeMillis();

        CacheNodeRuntimeMetadata cacheNodeRuntimeMetadata = new CacheNodeRuntimeMetadata();
        cacheNodeRuntimeMetadata.setId(id);
        cacheNodeRuntimeMetadata.setLastHeartbeatTimestamp(now);
        cacheNodeRuntimeMetadata.setStatus(CacheNodeStatus.REGISTERING);
        cacheNodeRuntimeMetadata.setFailedTimes(0);
        metadataRepository.updateCacheNodeRuntimeMetadata(cacheNodeRuntimeMetadata);

        CacheNodeMetadata cacheNodeMetadata = CacheNodeMetadata.builder()
                .id(id)
                .ip(ip)
                .port(port == null ? 6379 : port)
                .startupTimestamp(now)
                .version(1L)
                .build();
        metadataRepository.register(cacheNodeMetadata);
        updateMigrationStatus(cacheNodeMetadata, CacheNodeMigrationStatus.DUAL_WRITE_READ_OLD);
        waitForMigrationMetadataPropagation();
        migrateDataToNewNode(cacheNodeMetadata);

        cacheNodeRuntimeMetadata.setStatus(CacheNodeStatus.ONLINE);
        cacheNodeRuntimeMetadata.setLastHeartbeatTimestamp(System.currentTimeMillis());
        metadataRepository.updateCacheNodeRuntimeMetadata(cacheNodeRuntimeMetadata);
        updateMigrationStatus(cacheNodeMetadata, CacheNodeMigrationStatus.DUAL_WRITE_READ_NEW);
        waitForMigrationMetadataPropagation();
        updateMigrationStatus(cacheNodeMetadata, CacheNodeMigrationStatus.READ_NEW_ONLY);
        waitForMigrationMetadataPropagation();
        cleanMigratedDataFromOldNodes(cacheNodeMetadata);
        metadataRepository.deleteCacheNodeMigrationMetadata(cacheNodeMetadata.getId());
        log.info("Cache node:[{}] has been registered and set to ONLINE.", id);
    }

    private void updateMigrationStatus(CacheNodeMetadata newNode, CacheNodeMigrationStatus status) {
        CacheNodeMigrationMetadata migrationMetadata = CacheNodeMigrationMetadata.builder()
                .newNodeId(newNode.getId())
                .newNode(newNode)
                .status(status)
                .lastOperationTimestamp(System.currentTimeMillis())
                .build();
        metadataRepository.updateCacheNodeMigrationMetadata(migrationMetadata);
        log.info("Cache node:[{}] migration status has been updated to [{}].", newNode.getId(), status);
    }

    private void waitForMigrationMetadataPropagation() {
        try {
            Thread.sleep(MIGRATION_METADATA_PROPAGATION_DELAY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for migration metadata propagation.", e);
        }
    }

    private void migrateDataToNewNode(CacheNodeMetadata newNode) {
        Map<String, CacheNodeMetadata> allNodeMetadata = metadataRepository.getAllCacheNodeMetadata();
        Map<String, CacheNodeRuntimeMetadata> allRuntimeMetadata = metadataRepository.getAllCacheNodeRuntimeMetadata();
        List<CacheNodeMetadata> oldOnlineNodes = getOldOnlineNodes(newNode, allNodeMetadata, allRuntimeMetadata);
        if (oldOnlineNodes.isEmpty()) {
            log.info("No old online nodes found. Skip data migration for new node:[{}].", newNode.getId());
            return;
        }

        NavigableMap<Long, CacheNodeMetadata> oldHashRing = buildHashRing(oldOnlineNodes);
        List<CacheNodeMetadata> newRingNodes = new ArrayList<>(oldOnlineNodes);
        newRingNodes.add(newNode);
        NavigableMap<Long, CacheNodeMetadata> newHashRing = buildHashRing(newRingNodes);
        if (oldHashRing.isEmpty() || newHashRing.isEmpty()) {
            log.info("Hash ring is empty. Skip data migration for new node:[{}].", newNode.getId());
            return;
        }

        for (CacheNodeMetadata sourceNode : oldOnlineNodes) {
            migrateNodeData(sourceNode, newNode, oldHashRing, newHashRing);
        }
    }

    private void cleanMigratedDataFromOldNodes(CacheNodeMetadata newNode) {
        Map<String, CacheNodeMetadata> allNodeMetadata = metadataRepository.getAllCacheNodeMetadata();
        Map<String, CacheNodeRuntimeMetadata> allRuntimeMetadata = metadataRepository.getAllCacheNodeRuntimeMetadata();
        List<CacheNodeMetadata> oldOnlineNodes = getOldOnlineNodes(newNode, allNodeMetadata, allRuntimeMetadata);
        if (oldOnlineNodes.isEmpty()) {
            return;
        }

        NavigableMap<Long, CacheNodeMetadata> oldHashRing = buildHashRing(oldOnlineNodes);
        List<CacheNodeMetadata> newRingNodes = new ArrayList<>(oldOnlineNodes);
        newRingNodes.add(newNode);
        NavigableMap<Long, CacheNodeMetadata> newHashRing = buildHashRing(newRingNodes);
        if (oldHashRing.isEmpty() || newHashRing.isEmpty()) {
            return;
        }

        for (CacheNodeMetadata sourceNode : oldOnlineNodes) {
            cleanNodeMigratedData(sourceNode, newNode, oldHashRing, newHashRing);
        }
    }

    private List<CacheNodeMetadata> getOldOnlineNodes(
            CacheNodeMetadata newNode,
            Map<String, CacheNodeMetadata> allNodeMetadata,
            Map<String, CacheNodeRuntimeMetadata> allRuntimeMetadata) {
        List<CacheNodeMetadata> onlineNodes = new ArrayList<>();
        if (MapUtils.isEmpty(allNodeMetadata) || MapUtils.isEmpty(allRuntimeMetadata)) {
            return onlineNodes;
        }

        for (Map.Entry<String, CacheNodeMetadata> entry : allNodeMetadata.entrySet()) {
            String nodeId = entry.getKey();
            if (Strings.CS.equals(nodeId, newNode.getId())) {
                continue;
            }
            CacheNodeRuntimeMetadata runtimeMetadata = allRuntimeMetadata.get(nodeId);
            if (runtimeMetadata != null && CacheNodeStatus.isOnline(runtimeMetadata.getStatus())) {
                onlineNodes.add(entry.getValue());
            }
        }
        return onlineNodes;
    }

    private void migrateNodeData(
            CacheNodeMetadata sourceNode,
            CacheNodeMetadata newNode,
            NavigableMap<Long, CacheNodeMetadata> oldHashRing,
            NavigableMap<Long, CacheNodeMetadata> newHashRing) {
        Set<String> keys = businessRepository.scanKeys(sourceNode);
        if (keys.isEmpty()) {
            return;
        }

        for (String key : keys) {
            CacheNodeMetadata oldOwner = route(key, oldHashRing);
            CacheNodeMetadata newOwner = route(key, newHashRing);
            if (oldOwner == null || newOwner == null) {
                continue;
            }
            if (!Strings.CS.equals(oldOwner.getId(), sourceNode.getId())
                    || !Strings.CS.equals(newOwner.getId(), newNode.getId())) {
                continue;
            }
            moveKey(key, sourceNode, newNode);
        }
    }

    private void cleanNodeMigratedData(
            CacheNodeMetadata sourceNode,
            CacheNodeMetadata newNode,
            NavigableMap<Long, CacheNodeMetadata> oldHashRing,
            NavigableMap<Long, CacheNodeMetadata> newHashRing) {
        Set<String> keys = businessRepository.scanKeys(sourceNode);
        if (keys.isEmpty()) {
            return;
        }

        for (String key : keys) {
            CacheNodeMetadata oldOwner = route(key, oldHashRing);
            CacheNodeMetadata newOwner = route(key, newHashRing);
            if (oldOwner == null || newOwner == null) {
                continue;
            }
            if (!Strings.CS.equals(oldOwner.getId(), sourceNode.getId())
                    || !Strings.CS.equals(newOwner.getId(), newNode.getId())) {
                continue;
            }
            businessRepository.del(key, sourceNode);
            log.info("Clean migrated key:[{}] from old node:[{}].", key, sourceNode.getId());
        }
    }

    private void moveKey(String key, CacheNodeMetadata sourceNode, CacheNodeMetadata targetNode) {
        String value = businessRepository.get(key, sourceNode);
        if (value == null) {
            return;
        }

        Long ttl = businessRepository.ttl(key, sourceNode);
        if (ttl != null && ttl == -2L) {
            return;
        }
        if (ttl != null && ttl > 0) {
            businessRepository.setEx(key, value, ttl > Integer.MAX_VALUE ? Integer.MAX_VALUE : ttl.intValue(), targetNode);
        } else {
            businessRepository.set(key, value, targetNode);
        }
        log.info("Copy key:[{}] from node:[{}] to new node:[{}].", key, sourceNode.getId(), targetNode.getId());
    }

    private NavigableMap<Long, CacheNodeMetadata> buildHashRing(Collection<CacheNodeMetadata> nodes) {
        NavigableMap<Long, CacheNodeMetadata> hashRing = new TreeMap<>();
        for (CacheNodeMetadata node : nodes) {
            if (node == null) {
                continue;
            }
            for (int i = 0; i < HashRingConstants.VIRTUAL_NODE_COUNT; i++) {
                String virtualNodeId = node.getId() + "#VNODE-" + i;
                Long virtualNodeHashValue = HashUtils.hash(virtualNodeId);
                hashRing.putIfAbsent(virtualNodeHashValue, node);
            }
        }
        return hashRing;
    }

    private CacheNodeMetadata route(String key, NavigableMap<Long, CacheNodeMetadata> hashRing) {
        long hash = HashUtils.hash(key);
        Map.Entry<Long, CacheNodeMetadata> targetEntry = hashRing.ceilingEntry(hash);
        if (targetEntry == null) {
            targetEntry = hashRing.firstEntry();
        }
        return targetEntry == null ? null : targetEntry.getValue();
    }
}
