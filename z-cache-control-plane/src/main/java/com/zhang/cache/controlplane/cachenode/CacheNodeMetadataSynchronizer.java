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
package com.zhang.cache.controlplane.cachenode;

import com.zhang.cache.core.metadata.cachenode.CacheNodeMetadataManager;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMetadata;
import com.zhang.cache.core.repository.MetadataRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author zzzhangyxi
 * @since 2026/6/26
 */
@Slf4j
@Component
public class CacheNodeMetadataSynchronizer {
    @Autowired
    private MetadataRepository metadataRepository;
    @Autowired
    private CacheNodeMetadataManager cacheNodeMetadataManager;

    private long lastRefreshTime = 0L;

    /**
     * Do not need to use ConcurrentHashMap, HashMap is enough for a metadata read and update scenario.<br>
     * This method is the only entrance that can update local metadata cache,
     * so concurrent issues does not exist here.<br>
     * Volatile keyword is necessary to avoid visibility issues.
     */
    @Scheduled(fixedRate = 1000)
    public synchronized void startScheduledRefreshLocalMetadata() {
        log.info("start to schedule refresh local metadata...");

        // use reference replacing to avoid concurrent issues and visibility issues.
        refreshLocalMetadata();
    }

    private void refreshLocalMetadata() {
        long refreshTimestamp = System.currentTimeMillis();
        Map<String, CacheNodeMetadata> cacheNodeMetadata = metadataRepository.getAllCacheNodeMetadata();
        if (refreshTimestamp > lastRefreshTime) {
            cacheNodeMetadataManager.setCacheNodeMetadata(cacheNodeMetadata);
            lastRefreshTime = refreshTimestamp;
        } else {
            log.info("Refresh time:{}, last refresh time:{}, ignore this refresh.", refreshTimestamp, lastRefreshTime);
        }
    }
}
