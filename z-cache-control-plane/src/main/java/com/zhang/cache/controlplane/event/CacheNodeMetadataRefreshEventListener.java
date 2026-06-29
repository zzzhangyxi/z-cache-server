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
package com.zhang.cache.controlplane.event;

import com.zhang.cache.core.event.EventListener;
import com.zhang.cache.core.event.entity.CacheNodeMetadataRefreshEvent;
import com.zhang.cache.core.hash.HashRingManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author zzzhangyxi
 * @since 2026/6/29
 */
@Component
public class CacheNodeMetadataRefreshEventListener implements EventListener<CacheNodeMetadataRefreshEvent> {
    @Autowired
    private HashRingManager hashRingManager;

    @Override
    public Class<CacheNodeMetadataRefreshEvent> supportType() {
        return CacheNodeMetadataRefreshEvent.class;
    }

    @Override
    public void onEvent(CacheNodeMetadataRefreshEvent event) {
        hashRingManager.rebuildHashRing();
    }
}
