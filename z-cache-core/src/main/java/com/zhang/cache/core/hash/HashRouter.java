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
import com.zhang.cache.core.metadata.cachenode.CacheNodeMetadataManager;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMetadata;
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

    public CacheNodeMetadata route(String key) {
        HotKeyReplicationMetadata replication = getHotKeyReplicationMetadata(key);
        if (isHotKey(replication)) {
            return hotKeyRoute(key, replication);
        }
        return basicRoute(key);
    }

    public CacheNodeMetadata basicRoute(String key) {
        long hash = HashUtils.hash(key);
        NavigableMap<Long, CacheNodeMetadata> hashRing = hashRingManager.getHashRing();
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
}
