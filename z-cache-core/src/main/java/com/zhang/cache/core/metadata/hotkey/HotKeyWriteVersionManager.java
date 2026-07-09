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
package com.zhang.cache.core.metadata.hotkey;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zzzhangyxi
 * @since 2026/7/9
 */
@Component
public class HotKeyWriteVersionManager {
    private final ConcurrentMap<String, AtomicLong> writeVersions = new ConcurrentHashMap<>();

    public void markWritten(String key) {
        if (StringUtils.isBlank(key)) {
            return;
        }
        writeVersions.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
    }

    public Map<String, Long> snapshot() {
        Map<String, Long> snapshot = new HashMap<>();
        writeVersions.forEach((key, version) -> snapshot.put(key, version.longValue()));
        return snapshot;
    }
}
