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
package com.zhang.cache.dataplane.event.listener;

import com.zhang.cache.core.event.EventListener;
import com.zhang.cache.core.metadata.hotkey.HotKeyReplicationMetadataManager;
import com.zhang.cache.core.metadata.hotkey.HotKeyReplicationStatus;
import com.zhang.cache.core.metadata.hotkey.entity.HotKeyReplicationMetadata;
import com.zhang.cache.dataplane.event.entity.WriteKeyEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zzzhangyxi
 * @since 2026/5/16
 */
@Component
@Slf4j
public class WriteKeyEventListener implements EventListener<WriteKeyEvent> {
    @Autowired
    private HotKeyReplicationMetadataManager hotKeyReplicationMetadataManager;

    @Override
    public Class<WriteKeyEvent> supportType() {
        return WriteKeyEvent.class;
    }

    @Override
    public void onEvent(WriteKeyEvent event) {
        if (event == null || StringUtils.isBlank(event.getKey())) {
            return;
        }

        String key = event.getKey();
        Map<String, HotKeyReplicationMetadata> currentReplicationMetadata =
                hotKeyReplicationMetadataManager.getReplicationMetadata();
        if (MapUtils.isEmpty(currentReplicationMetadata)) {
            return;
        }

        HotKeyReplicationMetadata replicationMetadata = currentReplicationMetadata.get(key);
        if (replicationMetadata == null || CollectionUtils.isEmpty(replicationMetadata.getReplicationNodes())) {
            return;
        }

        replicationMetadata.setStatus(HotKeyReplicationStatus.INVALIDATING);
        replicationMetadata.setLastOperationTimestamp(System.currentTimeMillis());
        refreshLocalReplicationMetadata(replicationMetadata);
        log.info("Hot key:[{}] replica metadata has been marked as INVALIDATING by write event.", key);
    }

    private void refreshLocalReplicationMetadata(HotKeyReplicationMetadata replicationMetadata) {
        Map<String, HotKeyReplicationMetadata> currentReplicationMetadata =
                hotKeyReplicationMetadataManager.getReplicationMetadata();
        Map<String, HotKeyReplicationMetadata> refreshedReplicationMetadata = new HashMap<>();
        if (currentReplicationMetadata != null) {
            refreshedReplicationMetadata.putAll(currentReplicationMetadata);
        }
        refreshedReplicationMetadata.put(replicationMetadata.getKey(), replicationMetadata);
        hotKeyReplicationMetadataManager.setReplicationMetadata(refreshedReplicationMetadata);
    }
}
