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

import com.zhang.cache.core.constant.HashRingConstants;
import com.zhang.cache.core.metadata.MetadataManager;
import com.zhang.cache.core.metadata.cachenode.CacheNodeMetadata;
import com.zhang.cache.core.metadata.cachenode.CacheNodeStatus;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
@Component
@Slf4j
public class HashRingManager {
    @Getter
    private volatile NavigableMap<Long, CacheNodeMetadata> hashRing = new TreeMap<>();

    @Autowired
    private MetadataManager metadataManager;

    public void rebuildHashRing() {
        NavigableMap<Long, CacheNodeMetadata> newHashRing = new TreeMap<>();

        Map<String, CacheNodeMetadata> cacheNodeMetadata = metadataManager.getAllCacheNodeMetadata();
        for (Map.Entry<String, CacheNodeMetadata> metadataEntry : cacheNodeMetadata.entrySet()) {
            CacheNodeMetadata nodeMetadata = metadataEntry.getValue();

            if (CacheNodeStatus.isNotOnline(nodeMetadata.getStatus())) {
                // only online nodes should be added into the hash ring
                continue;
            }

            for (int i = 0; i < HashRingConstants.VIRTUAL_NODE_COUNT; i++) {
                String virtualNodeId = nodeMetadata.getId() + "#VNODE-" + i;
                Long virtualNodeHashValue = HashUtils.hash(virtualNodeId);

                CacheNodeMetadata originalNode = newHashRing.get(virtualNodeHashValue);
                if (originalNode != null) {
                    /* Hash collision is an extremely rare undercurrent v-node scale.
                     * Ignore collided v-node to preserve deterministic ring topology.
                     */
                    log.warn("Hash collision detected. VNode hash:{}, original node:{}, new node:{}",
                            virtualNodeHashValue, originalNode.getId(), nodeMetadata.getId());
                    continue;
                }
                newHashRing.put(virtualNodeHashValue, nodeMetadata);
            }
        }

        // reference replace
        hashRing = newHashRing;
    }
}
