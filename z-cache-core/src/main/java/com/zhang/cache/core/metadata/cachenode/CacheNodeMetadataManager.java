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
package com.zhang.cache.core.metadata.cachenode;

import com.zhang.cache.core.event.EventPublisher;
import com.zhang.cache.core.event.entity.CacheNodeMetadataRefreshEvent;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMetadata;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zzzhangyxi
 * @since 2026/5/11
 */
@Slf4j
@Component
public class CacheNodeMetadataManager {
    @Autowired
    private EventPublisher eventPublisher;

    @Getter
    private volatile Map<String, CacheNodeMetadata> cacheNodeMetadata = new ConcurrentHashMap<>();

    public void setCacheNodeMetadata(Map<String, CacheNodeMetadata> cacheNodeMetadata) {
        this.cacheNodeMetadata = cacheNodeMetadata;
        eventPublisher.publishEvent(new CacheNodeMetadataRefreshEvent());
    }
}
