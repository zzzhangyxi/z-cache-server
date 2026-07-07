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
import com.zhang.cache.core.metadata.hotkey.HotKeyMetadataManager;
import com.zhang.cache.core.metadata.hotkey.HotKeyStatus;
import com.zhang.cache.core.metadata.hotkey.entity.HotKeyMetadata;
import com.zhang.cache.core.metadata.hotkey.entity.HotKeyReplicationMetadata;
import com.zhang.cache.core.repository.BusinessRepository;
import com.zhang.cache.core.repository.MetadataRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
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
    private HotKeyMetadataManager hotKeyMetadataManager;
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
            int nodeThreshold = allHotKeyMetadata.size() / 2;
            Map<String, Integer> hotKeyCounter = new HashMap<>();

            for (Map<String, HotKeyMetadata> hotKeys : allHotKeyMetadata.values()) {
                Collection<HotKeyMetadata> hotKeyMetadata = hotKeys.values();
                if (CollectionUtils.isEmpty(hotKeyMetadata)) {
                    continue;
                }
                hotKeys.forEach((key, metadata) -> {
                    HotKeyStatus hotKeyStatus = metadata.getStatus();
                    if (HotKeyStatus.ACTIVE.equals(hotKeyStatus)) {
                        Integer count = hotKeyCounter.getOrDefault(key, 0);
                        hotKeyCounter.put(key, count + 1);
                    }
                });
            }

            for (Map.Entry<String, Integer> counter : hotKeyCounter.entrySet()) {
                if (counter.getValue() >= nodeThreshold) {
                    doReplicateHotKey(counter.getKey());
                }
            }
            // TODO 冷key删除
        } catch (Exception e) {
            log.error("Processing replication failed", e);
        } finally {
            metadataRepository.unlockForReplication();
        }
    }

    private void doReplicateHotKey(String hotKey) {
        long now = System.currentTimeMillis();
        HotKeyReplicationMetadata replicationMetadata = metadataRepository.getHotKeyReplicationMetadata(hotKey);
        if (replicationMetadata == null) {
            Map<String, CacheNodeMetadata> onlineNodes = cacheNodeMetadataManager.getOnlineNodes();
            CacheNodeMetadata originNode = hashRouter.basicRoute(hotKey);
            List<CacheNodeMetadata> replicaNodes = getReplicaNodes(hotKey, originNode, onlineNodes.size());
            log.info("replicaNodes = {}", replicaNodes);

            String businessValue = businessRepository.get(hotKey, originNode);
            for (CacheNodeMetadata replicaNode : replicaNodes) {
                businessRepository.set(hotKey, businessValue, replicaNode);
                HotKeyReplicationMetadata hotKeyReplicationMetadata = HotKeyReplicationMetadata.builder()
                        .key(hotKey)
                        .replicationNodes(replicaNodes)
                        .lastOperationTimestamp(now)
                        .build();
                metadataRepository.updateHotKeyReplicaNodes(hotKeyReplicationMetadata);
            }
        } else {
            log.info("Hot key:[{}] has already been replicated. Do not need to handle it duplicately.", hotKey);
        }
    }

    private List<CacheNodeMetadata> getReplicaNodes(String hotKey, CacheNodeMetadata originalNode, int totalNodesCount) {
        long qps = hotKeyMetadataManager.getHotKeyAverageQps(hotKey);
        int replicaCount = Math.toIntExact(Math.min(totalNodesCount, qps / totalNodesCount));

        List<CacheNodeMetadata> replicaNodes = new ArrayList<>();
        int replicaIndex = -1;
        CacheNodeMetadata node = originalNode;
        while (replicaNodes.size() < replicaCount - 1) {
            while (node == originalNode || replicaNodes.contains(node)) {
                replicaIndex++;
                String key = hotKey + "#" + replicaIndex;
                node = hashRouter.basicRoute(key);
            }
            replicaNodes.add(node);
        }
        return replicaNodes;
    }
}
