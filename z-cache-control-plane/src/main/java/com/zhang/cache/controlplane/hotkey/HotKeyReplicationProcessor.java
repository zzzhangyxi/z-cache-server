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
package com.zhang.cache.controlplane.hotkey;

import com.zhang.cache.core.hash.HashRouter;
import com.zhang.cache.core.metadata.cachenode.CacheNodeMetadataManager;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMetadata;
import com.zhang.cache.core.metadata.hotkey.HotKeyReplicationStatus;
import com.zhang.cache.core.metadata.hotkey.HotKeyStatus;
import com.zhang.cache.core.metadata.hotkey.entity.HotKeyMetadata;
import com.zhang.cache.core.metadata.hotkey.entity.HotKeyReplicationMetadata;
import com.zhang.cache.core.repository.BusinessRepository;
import com.zhang.cache.core.repository.MetadataRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zzzhangyxi
 * @since 2026/5/16
 */
@Component
@Slf4j
public class HotKeyReplicationProcessor {
    private static final long REPLICA_DELETE_DELAY_MILLIS = 10000L;

    @Autowired
    private CacheNodeMetadataManager cacheNodeMetadataManager;
    @Autowired
    private MetadataRepository metadataRepository;
    @Autowired
    private HashRouter hashRouter;
    @Autowired
    private BusinessRepository businessRepository;

    /**
     * The server cluster is a stateless cluster, which means every node runs equally.
     * Therefore, there is not a primary node to finish the replication job, every node has a chance to do this.
     * However, this logic can only be executed once at the same time, so a distributed lock is required to ensure
     * only one executor can do the scheduling.
     */
    @Scheduled(fixedRate = 5000)
    public void replicateDetectedHotKeys() {
        long now = System.currentTimeMillis();
        // Try to get the distributed lock.
        boolean lockSuccess = metadataRepository.lockForReplication();
        if (lockSuccess) {
            log.info("Get the distributed lock successfully. Start scheduling replication.");
        } else {
            log.info("Get the distributed lock failed. Do not start scheduling replication.");
            return;
        }

        try {
            Map<String, Map<String, HotKeyMetadata>> allHotKeyMetadata = metadataRepository.getAllHotKeyMetadata();
            Map<String, HotKeyReplicationMetadata> allReplicationMetadata = metadataRepository.getAllHotKeyReplicationMetadata();
            if (MapUtils.isEmpty(allHotKeyMetadata) && MapUtils.isEmpty(allReplicationMetadata)) {
                log.info("No hot key metadata or replica metadata found. Do not start scheduling replication.");
                return;
            }

            int nodeThreshold = Math.max(1, allHotKeyMetadata.size() / 2 + 1);
            Map<String, Integer> hotKeyCounter = new HashMap<>();
            Set<String> activeOrCoolingDownKeys = new HashSet<>();

            if (MapUtils.isNotEmpty(allHotKeyMetadata)) {
                for (Map<String, HotKeyMetadata> hotKeys : allHotKeyMetadata.values()) {
                    if (MapUtils.isEmpty(hotKeys)) {
                        continue;
                    }
                    hotKeys.forEach((key, metadata) -> {
                        if (metadata == null) {
                            return;
                        }
                        HotKeyStatus hotKeyStatus = metadata.getStatus();
                        if (HotKeyStatus.ACTIVE.equals(hotKeyStatus)) {
                            Integer count = hotKeyCounter.getOrDefault(key, 0);
                            hotKeyCounter.put(key, count + 1);
                            activeOrCoolingDownKeys.add(key);
                        } else if (HotKeyStatus.COOLING_DOWN.equals(hotKeyStatus)) {
                            activeOrCoolingDownKeys.add(key);
                        }
                    });
                }
            }

            processReplicationLifecycle(allReplicationMetadata, activeOrCoolingDownKeys, now);

            for (Map.Entry<String, Integer> counter : hotKeyCounter.entrySet()) {
                int activeNodeCount = counter.getValue();
                if (activeNodeCount >= nodeThreshold) {
                    doReplicateHotKey(counter.getKey(), activeNodeCount);
                }
            }
        } catch (Exception e) {
            log.error("Processing replication failed", e);
        } finally {
            metadataRepository.unlockForReplication();
        }
    }

    private void processReplicationLifecycle(
            Map<String, HotKeyReplicationMetadata> allReplicationMetadata,
            Set<String> activeOrCoolingDownKeys,
            long now) {
        if (MapUtils.isEmpty(allReplicationMetadata)) {
            return;
        }

        for (Map.Entry<String, HotKeyReplicationMetadata> entry : allReplicationMetadata.entrySet()) {
            String hotKey = entry.getKey();
            if (activeOrCoolingDownKeys.contains(hotKey)) {
                restoreReadyIfNecessary(hotKey, entry.getValue(), now);
                continue;
            }
            cleanColdHotKey(hotKey, entry.getValue(), now);
        }
    }

    private void restoreReadyIfNecessary(String hotKey, HotKeyReplicationMetadata replicationMetadata, long now) {
        if (replicationMetadata == null || !HotKeyReplicationStatus.DELETING.equals(replicationMetadata.getStatus())) {
            return;
        }
        replicationMetadata.setStatus(HotKeyReplicationStatus.READY);
        replicationMetadata.setLastOperationTimestamp(now);
        metadataRepository.updateHotKeyReplicaNodes(replicationMetadata);
        log.info("Hot key:[{}] becomes active again. Restore replica metadata to READY.", hotKey);
    }

    private void cleanColdHotKey(String hotKey, HotKeyReplicationMetadata replicationMetadata, long now) {
        if (replicationMetadata == null || CollectionUtils.isEmpty(replicationMetadata.getReplicationNodes())) {
            metadataRepository.deleteHotKeyReplicationMetadata(hotKey);
            log.info("No replica nodes found for cold hot key:[{}].", hotKey);
            return;
        }

        HotKeyReplicationStatus status = replicationMetadata.getStatus();
        if (status == null || HotKeyReplicationStatus.READY.equals(status)) {
            replicationMetadata.setStatus(HotKeyReplicationStatus.DELETING);
            replicationMetadata.setLastOperationTimestamp(now);
            metadataRepository.updateHotKeyReplicaNodes(replicationMetadata);
            log.info("Cold hot key:[{}] replica metadata has been marked as DELETING.", hotKey);
            return;
        }

        if (now - replicationMetadata.getLastOperationTimestamp() < REPLICA_DELETE_DELAY_MILLIS) {
            return;
        }

        for (CacheNodeMetadata replicaNode : replicationMetadata.getReplicationNodes()) {
            if (replicaNode == null) {
                continue;
            }
            businessRepository.del(hotKey, replicaNode);
        }
        metadataRepository.deleteHotKeyReplicationMetadata(hotKey);
        log.info("Cold hot key:[{}] replica data has been cleaned.", hotKey);
    }

    private void doReplicateHotKey(String hotKey, int activeNodeCount) {
        long now = System.currentTimeMillis();
        HotKeyReplicationMetadata replicationMetadata = metadataRepository.getHotKeyReplicationMetadata(hotKey);
        if (replicationMetadata == null) {
            Map<String, CacheNodeMetadata> onlineNodes = cacheNodeMetadataManager.getOnlineNodes();
            if (MapUtils.isEmpty(onlineNodes) || onlineNodes.size() <= 1) {
                log.info("No available replica nodes found for hot key:[{}].", hotKey);
                return;
            }

            CacheNodeMetadata originNode = hashRouter.basicRoute(hotKey);
            List<CacheNodeMetadata> replicaNodes = getReplicaNodes(hotKey, originNode, onlineNodes, activeNodeCount);
            log.info("replicaNodes = {}", replicaNodes);
            if (CollectionUtils.isEmpty(replicaNodes)) {
                log.info("No replica nodes selected for hot key:[{}].", hotKey);
                return;
            }

            String businessValue = businessRepository.get(hotKey, originNode);
            if (businessValue == null) {
                return;
            }
            for (CacheNodeMetadata replicaNode : replicaNodes) {
                businessRepository.set(hotKey, businessValue, replicaNode);
            }
            HotKeyReplicationMetadata hotKeyReplicationMetadata = HotKeyReplicationMetadata.builder()
                    .key(hotKey)
                    .replicationNodes(replicaNodes)
                    .status(HotKeyReplicationStatus.READY)
                    .lastOperationTimestamp(now)
                    .build();
            metadataRepository.updateHotKeyReplicaNodes(hotKeyReplicationMetadata);
        } else {
            log.info("Hot key:[{}] has already been replicated. Do not need to handle it duplicately.", hotKey);
        }
    }

    private List<CacheNodeMetadata> getReplicaNodes(
            String hotKey,
            CacheNodeMetadata originalNode,
            Map<String, CacheNodeMetadata> onlineNodes,
            int activeNodeCount) {
        List<CacheNodeMetadata> replicaNodes = new ArrayList<>();
        if (originalNode == null || MapUtils.isEmpty(onlineNodes) || onlineNodes.size() <= 1) {
            return replicaNodes;
        }

        int replicaCount = Math.min(onlineNodes.size() - 1, activeNodeCount - 1);
        if (replicaCount <= 0) {
            return replicaNodes;
        }

        List<CacheNodeMetadata> candidates = new ArrayList<>();
        for (CacheNodeMetadata node : onlineNodes.values()) {
            if (node == null || Strings.CS.equals(node.getId(), originalNode.getId())) {
                continue;
            }
            candidates.add(node);
        }
        if (CollectionUtils.isEmpty(candidates)) {
            return replicaNodes;
        }

        candidates.sort(Comparator.comparing(left -> String.valueOf(left.getId())));
        int targetReplicaCount = Math.min(replicaCount, candidates.size());
        int startIndex = Math.floorMod(hotKey.hashCode(), candidates.size());
        for (int i = 0; i < targetReplicaCount; i++) {
            replicaNodes.add(candidates.get((startIndex + i) % candidates.size()));
        }
        return replicaNodes;
    }
}
