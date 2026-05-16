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
package com.zhang.cache.plane.control;

import com.zhang.cache.core.exception.NoAvailableNodeException;
import com.zhang.cache.core.hash.HashRingManager;
import com.zhang.cache.core.hash.HashUtils;
import com.zhang.cache.core.hotkey.HotKeyManager;
import com.zhang.cache.core.metadata.cachenode.CacheNodeMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.NavigableMap;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
@Slf4j
@Service
public class RoutingService {
    @Autowired
    private HashRingManager hashRingManager;
    @Autowired
    private HotKeyManager hotKeyManager;

    public CacheNodeMetadata route(String key) {
        boolean hotKey = hotKeyManager.isHotKey(key);
        return hotKey ? hotKeyRoute(key) : basicRoute(key);
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

    public CacheNodeMetadata hotKeyRoute(String key) {
        // TODO 热点key路由逻辑
        return null;
    }
}
