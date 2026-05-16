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
package com.zhang.cache.core.hotkey;

import com.zhang.cache.core.metadata.CacheNodeMetadataManager;
import com.zhang.cache.core.metadata.HotKeyMetadataManager;
import com.zhang.cache.core.metadata.cachenode.CacheNodeMetadata;
import com.zhang.cache.core.metadata.hotkey.HotKeyMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

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

    @Scheduled(fixedRate = 1000)
    public void replicateDetectedHotKeys() {
        Map<String, HotKeyMetadata> allHotKeys = hotKeyMetadataManager.getHotKeyMetadataMap();
        if (MapUtils.isEmpty(allHotKeys)) {
            log.info("No hot keys, do not need to replication.");
            return;
        }

        Map<String, CacheNodeMetadata> allNodes = cacheNodeMetadataManager.getAllCacheNodeMetadata();
        if (MapUtils.isEmpty(allNodes) || allNodes.size() == 1) {
            log.info("No available replica nodes found. Stop replication.");
            return;
        }

        System.out.println("replicating......");
    }
}
