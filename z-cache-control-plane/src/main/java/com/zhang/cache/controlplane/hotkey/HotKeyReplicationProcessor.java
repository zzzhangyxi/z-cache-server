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
import com.zhang.cache.core.repository.MetadataRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author zzzhangyxi
 * @since 2026/5/16
 */
@Component
@Slf4j
public class HotKeyReplicationProcessor {
    /*@Autowired
    private CacheNodeMetadataManager cacheNodeMetadataManager;
    @Autowired
    private HotKeyMetadataManager hotKeyMetadataManager;
    @Autowired
    private MetadataRepository metadataRepository;
    @Autowired
    private HashRouter hashRouter;
    @Autowired
    private ReadWriteService readWriteService;

    @Scheduled(fixedRate = 1000)
    public void replicateDetectedHotKeys() {
        Map<String, HotKeyMetadata> allHotKeys = hotKeyMetadataManager.getHotKeyMetadata();
        if (MapUtils.isEmpty(allHotKeys)) {
            log.info("No hot keys, do not need to replication.");
            return;
        }

        List<String> detectedHotKeys = allHotKeys.entrySet()
                .stream()
                .filter(entry -> HotKeyStatus.DETECTED.equals(entry.getValue().getStatus()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(detectedHotKeys)) {
            log.info("No new hot keys, do not need to replication.");
            return;
        }

        Map<String, CacheNodeMetadata> allNodes = cacheNodeMetadataManager.getAllCacheNodeMetadata();
        if (MapUtils.isEmpty(allNodes) || allNodes.size() == 1) {
            log.info("No available replica nodes found. Stop replication.");
            return;
        }

        int nodesCount = allHotKeys.size();
        for (String hotKey : detectedHotKeys) {
            String lockId = metadataRepository.lockForReplication(hotKey);
            if (StringUtils.isBlank(lockId)) {
                continue;
            }

            long now = System.currentTimeMillis();
            try {
                CacheNodeMetadata originalNode = hashRouter.basicRoute(hotKey);
                String originValue = readWriteService.get(hotKey, originalNode);

                List<CacheNodeMetadata> replicaNodes = getReplicaNodes(hotKey, originalNode, nodesCount);
                for (CacheNodeMetadata replicaNode : replicaNodes) {
                    readWriteService.set(hotKey, originValue, replicaNode);
                }
                HotKeyMetadata hotKeyMetadata = HotKeyMetadata.builder()
                        .key(hotKey)
                        .status(HotKeyStatus.ACTIVE)
                        .lastOperationTimestamp(now)
                        .build();
                metadataRepository.updateHotKeyMetadata(hotKeyMetadata);

                replicaNodes.add(originalNode);
                HotKeyReplicationMetadata hotKeyReplicationMetadata = HotKeyReplicationMetadata.builder()
                        .key(hotKey)
                        .replicationNodes(replicaNodes)
                        .lastOperationTimestamp(now)
                        .build();
                metadataRepository.updateHotKeyReplicaNodes(hotKeyReplicationMetadata);
            } finally {
                metadataRepository.unlockForReplication(hotKey, lockId);
            }
        }
    }

    private List<CacheNodeMetadata> getReplicaNodes(String hotKey, CacheNodeMetadata originalNode, int totalNodesCount) {
        long qps = hotKeyMetadataManager.getHotKeyAverageQps(hotKey);
        int replicaCount = Math.toIntExact(Math.min(totalNodesCount, qps / totalNodesCount));

        List<CacheNodeMetadata> replicaNodes = new ArrayList<>();
        int replicaIndex = 0;
        while (replicaNodes.size() < replicaCount - 1) {
            String key = hotKey + "#" + replicaIndex;
            CacheNodeMetadata node = originalNode;
            while (node == originalNode || replicaNodes.contains(node)) {
                node = hashRouter.basicRoute(key);
            }
            replicaNodes.add(node);
        }
        return replicaNodes;
    }*/
}
