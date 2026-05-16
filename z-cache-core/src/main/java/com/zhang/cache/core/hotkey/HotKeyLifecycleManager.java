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

import com.zhang.cache.core.metadata.hotkey.HotKeyMetadata;
import com.zhang.cache.core.metadata.hotkey.HotKeyStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author zzzhangyxi
 * @since 2026/5/16
 */
@Component
@Slf4j
public class HotKeyLifecycleManager {
    private Map<String, HotKeyMetadata> hotKeyMetadataMap = new HashMap<>();

    public void updateLocalHotKeyMetadata(Set<String> hotKeys) {
        Set<String> lastHotKeys = new HashSet<>(hotKeyMetadataMap.keySet());
        Set<String> newHotKeys = new HashSet<>(hotKeys);
        newHotKeys.removeAll(lastHotKeys);

        long now = System.currentTimeMillis();
        Map<String, HotKeyMetadata> newHotKeyMetadata = new HashMap<>(hotKeys.size());
        for (String latestHotKey : hotKeys) {
            if (newHotKeys.contains(latestHotKey)) {
                HotKeyMetadata hotKeyMetadata = HotKeyMetadata.builder()
                        .key(latestHotKey)
                        .status(HotKeyStatus.DETECTED)
                        .lastOperationTimestamp(now)
                        .build();
                newHotKeyMetadata.put(latestHotKey, hotKeyMetadata);
            } else {
                HotKeyMetadata hotKeyMetadata = hotKeyMetadataMap.get(latestHotKey);
                if (hotKeyMetadata != null) {
                    // must not be null
                    HotKeyMetadata updatedHotKeyMetadata = HotKeyMetadata.builder()
                            .key(hotKeyMetadata.getKey())
                            .status(hotKeyMetadata.getStatus())
                            .lastOperationTimestamp(now)
                            .build();
                    newHotKeyMetadata.put(latestHotKey, updatedHotKeyMetadata);
                }
            }
        }

        hotKeyMetadataMap = newHotKeyMetadata;
    }

    // TODO COOLING_DOWN逻辑 & 数据复制逻辑
}
