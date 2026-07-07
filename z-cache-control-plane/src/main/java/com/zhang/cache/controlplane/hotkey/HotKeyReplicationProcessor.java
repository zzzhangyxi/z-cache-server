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
import java.util.List;
import java.util.Map;

/**
 * @author zzzhangyxi
 * @since 2026/5/16
 */
@Component
@Slf4j
public class HotKeyReplicationProcessor {
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
            if (MapUtils.isEmpty(allHotKeyMetadata)) {
                log.info("No hot key metadata found. Do not start scheduling replication.");
                return;
            }

            int nodeThreshold = Math.max(1, allHotKeyMetadata.size() / 2 + 1);
            Map<String, Integer> hotKeyCounter = new HashMap<>();

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
                    }
                });
            }

            for (Map.Entry<String, Integer> counter : hotKeyCounter.entrySet()) {
                int activeNodeCount = counter.getValue();
                if (activeNodeCount >= nodeThreshold) {
                    doReplicateHotKey(counter.getKey(), activeNodeCount);
                }
            }
            // TODO 冷key删除
        } catch (Exception e) {
            log.error("Processing replication failed", e);
        } finally {
            metadataRepository.unlockForReplication();
        }
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
