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
package com.zhang.cache.core.metadata;

import com.zhang.cache.core.event.EventPublisher;
import com.zhang.cache.core.event.entity.CacheNodeMetadataRefreshEvent;
import com.zhang.cache.core.metadata.cachenode.CacheNodeMetadata;
import com.zhang.cache.core.repository.MetadataRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zzzhangyxi
 * @since 2026/5/11
 */
@Slf4j
@Component
public class CacheNodeMetadataManager {
    @Autowired
    private MetadataRepository metadataRepository;
    @Autowired
    private EventPublisher eventPublisher;

    /**
     * Do not need to use ConcurrentHashMap, HashMap is enough for a metadata read and update scenario.<br>
     * {@link CacheNodeMetadataManager#startScheduledRefreshLocalMetadata()} is the only entrance which can update local metadata cache,
     * so concurrent issues does not exist here.<br>
     * Volatile keyword is necessary to avoid visibility issues.
     */
    private volatile Map<String, CacheNodeMetadata> cacheNodeMetadata = new HashMap<>();

    public Map<String, CacheNodeMetadata> getAllCacheNodeMetadata() {
        return cacheNodeMetadata;
    }

    @Scheduled(fixedRate = 5000)
    public void startScheduledRefreshLocalMetadata() {
        log.info("start to schedule refresh local metadata...");
        // use reference replacing to avoid concurrent issues and visibility issues.
        refreshLocalMetadata();
    }

    public void refreshLocalMetadata() {
        cacheNodeMetadata = metadataRepository.getAllCacheNodeMetadata();
        eventPublisher.publishEvent(new CacheNodeMetadataRefreshEvent());
    }
}
