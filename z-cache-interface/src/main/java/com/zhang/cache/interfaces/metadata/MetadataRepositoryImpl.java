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
package com.zhang.cache.interfaces.metadata;

import com.alibaba.fastjson.JSON;
import com.zhang.cache.core.constant.DistributedLockConstants;
import com.zhang.cache.core.metadata.cachenode.CacheNodeMetadata;
import com.zhang.cache.core.metadata.hotkey.HotKeyMetadata;
import com.zhang.cache.core.metadata.hotkey.HotKeyReplicationMetadata;
import com.zhang.cache.core.metadata.hotkey.HotKeyStatus;
import com.zhang.cache.core.repository.MetadataRepository;
import com.zhang.cache.interfaces.RedisConstants;
import com.zhang.cache.interfaces.metadata.lua.LuaScripts;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
@Repository
@Slf4j
public class MetadataRepositoryImpl implements MetadataRepository {
    @Autowired
    private RedisCommands<String, String> redisCommands;

    @Override
    public String lockForReplication(String key) {
        String lockId = UUID.randomUUID().toString();
        String metadataKey = RedisConstants.HOT_KEY_METADATA_KEY + key;
        String lockKey = DistributedLockConstants.LOCK_KEY_PREFIX + key;

        Long lockResult = redisCommands.eval(
                LuaScripts.LOCK_FOR_REPLICATION_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{metadataKey, lockKey},
                HotKeyStatus.DETECTED.name(),
                lockId,
                String.valueOf(10));

        boolean success = lockResult != null && lockResult == 1L;
        if (success) {
            return lockId;
        }
        return null;
    }

    @Override
    public void unlockForReplication(String hotKey, String lockId) {
        redisCommands.eval(
                LuaScripts.UNLOCK_FOR_REPLICATION_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{hotKey},
                lockId);
    }

    @Override
    public Map<String, CacheNodeMetadata> getAllCacheNodeMetadata() {
        Map<String, String> rawData = redisCommands.hgetall(RedisConstants.CACHE_NODE_METADATA_KEY);
        Map<String, CacheNodeMetadata> cacheNodeMetadata = new HashMap<>();
        if (MapUtils.isNotEmpty(rawData)) {
            rawData.forEach((nodeId, nodeMetadata) ->
                    cacheNodeMetadata.put(nodeId, JSON.parseObject(nodeMetadata, CacheNodeMetadata.class)));
        }
        log.info("Cache node metadata:[{}]", JSON.toJSONString(cacheNodeMetadata));
        return cacheNodeMetadata;
    }

    @Override
    public void register(CacheNodeMetadata cacheNodeMetadata) {
        String nodeId = cacheNodeMetadata.getId();
        String metadata = JSON.toJSONString(cacheNodeMetadata);
        redisCommands.hset(RedisConstants.CACHE_NODE_METADATA_KEY, nodeId, metadata);
    }

    @Override
    public Map<String, HotKeyMetadata> getAllHotKeyMetadata() {
        Map<String, String> rawData = redisCommands.hgetall(RedisConstants.HOT_KEY_METADATA_KEY);
        Map<String, HotKeyMetadata> hotKeyMetadata = new HashMap<>();
        if (MapUtils.isNotEmpty(rawData)) {
            hotKeyMetadata = parseHotKeyRawMetadata(rawData);
        }
        log.info("Cache hotkey metadata:[{}]", JSON.toJSONString(hotKeyMetadata));
        return hotKeyMetadata;
    }

    @Override
    public void updateHotKeyMetadata(HotKeyMetadata hotKeyMetadata) {
        String key = hotKeyMetadata.getKey();
        String hotKeyMetadataKey = RedisConstants.HOT_KEY_METADATA_KEY;

        // use the lua script to update metadata atomically, avoid new version data being covered by old version data.
        redisCommands.eval(
                LuaScripts.HOT_KEY_METADATA_UPDATE_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{hotKeyMetadataKey},
                String.valueOf(hotKeyMetadata.getLastOperationTimestamp()),
                key,
                hotKeyMetadata.getStatus().name());
    }

    @Override
    public void updateHotKeyReplicaNodes(HotKeyReplicationMetadata hotKeyReplicationMetadata) {
        String key = hotKeyReplicationMetadata.getKey();
        String hotKeyReplicationMetadataKey = RedisConstants.HOT_KEY_REPLICATION_PREFIX + key;

        redisCommands.eval(
                LuaScripts.HOT_KEY_REPLICATION_METADATA_UPDATE_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{hotKeyReplicationMetadataKey},
                String.valueOf(hotKeyReplicationMetadata.getLastOperationTimestamp()),
                key,
                JSON.toJSONString(hotKeyReplicationMetadata.getReplicationNodes()));
    }

    private Map<String, HotKeyMetadata> parseHotKeyRawMetadata(Map<String, String> rawData) {
        return rawData.entrySet()
                .stream()
                .map(entry -> {
                    if (entry == null) {
                        return null;
                    }
                    String key = entry.getKey();
                    String value = entry.getValue();
                    if (StringUtils.isBlank(key) || StringUtils.isBlank(value)) {
                        return null;
                    }
                    HotKeyMetadata metadata = new HotKeyMetadata();
                    metadata.setKey(key);
                    /* format of metadata value:
                     * key: test; value: {STATUS}|{TIMESTAMP}
                     * for example: ACTIVE|123456789
                     */
                    String[] values = value.split("\\|");
                    HotKeyStatus status;
                    long timestamp;
                    if (values.length == 2) {
                        String statusName = values[0];
                        String timestampStr = values[1];
                        status = HotKeyStatus.ofName(statusName);
                        if (status == null) {
                            log.error("Illegal status of hot key metadata:[{}], hot key:[{}]", statusName, key);
                            return null;
                        }
                        timestamp = Long.parseLong(timestampStr);
                        metadata.setStatus(status);
                        metadata.setLastOperationTimestamp(timestamp);
                    } else {
                        // illegal data, need to be filtered
                        log.error("Illegal format of hot key metadata:[{}], hot key:[{}]", value, key);
                        return null;
                    }

                    return metadata;
                }).filter(Objects::nonNull)
                .collect(Collectors.toMap(HotKeyMetadata::getKey, Function.identity()));
    }
}
