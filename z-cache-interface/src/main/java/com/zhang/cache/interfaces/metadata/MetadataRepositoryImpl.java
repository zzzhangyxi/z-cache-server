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
import com.alibaba.fastjson.TypeReference;
import com.zhang.cache.core.constant.DistributedLockConstants;
import com.zhang.cache.core.identity.NodeIdentityGenerator;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMetadata;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeRuntimeMetadata;
import com.zhang.cache.core.metadata.hotkey.entity.HotKeyMetadata;
import com.zhang.cache.core.metadata.hotkey.entity.HotKeyReplicationMetadata;
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

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
@Repository
@Slf4j
public class MetadataRepositoryImpl implements MetadataRepository {
    @Autowired
    private RedisCommands<String, String> redisCommands;
    @Autowired
    private NodeIdentityGenerator nodeIdentityGenerator;

    @Override
    public boolean lockForReplication() {
        Long lockResult = redisCommands.eval(
                LuaScripts.LOCK_FOR_REPLICATION_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{DistributedLockConstants.LOCK_KEY},
                nodeIdentityGenerator.getNodeId(),
                String.valueOf(60000)
        );
        return lockResult != null && lockResult == 1L;
    }

    @Override
    public void unlockForReplication() {
        redisCommands.eval(
                LuaScripts.UNLOCK_FOR_REPLICATION_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{DistributedLockConstants.LOCK_KEY},
                nodeIdentityGenerator.getNodeId()
        );
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
    public Map<String, CacheNodeRuntimeMetadata> getAllCacheNodeRuntimeMetadata() {
        Map<String, String> rawData = redisCommands.hgetall(RedisConstants.CACHE_NODE_RUNTIME_KEY);
        Map<String, CacheNodeRuntimeMetadata> cacheNodeRuntimeMetadata = new HashMap<>();
        if (MapUtils.isNotEmpty(rawData)) {
            rawData.forEach((nodeId, runtimeMetadata) ->  {
                if (StringUtils.isBlank(nodeId) || StringUtils.isBlank(runtimeMetadata)) {
                    return;
                }
                CacheNodeRuntimeMetadata runtime = JSON.parseObject(runtimeMetadata, CacheNodeRuntimeMetadata.class);
                cacheNodeRuntimeMetadata.put(nodeId, runtime);
            });
        }
        log.info("Heartbeat runtime metadata:[{}]", JSON.toJSONString(cacheNodeRuntimeMetadata));
        return cacheNodeRuntimeMetadata;
    }

    @Override
    public Map<String, Map<String, HotKeyMetadata>> getAllHotKeyMetadata() {
        Map<String, String> rawData = redisCommands.hgetall(RedisConstants.HOT_KEY_METADATA_KEY);
        Map<String, Map<String, HotKeyMetadata>> hotKeyMetadata = new HashMap<>();
        if (MapUtils.isNotEmpty(rawData)) {
            for (Map.Entry<String, String> entry : rawData.entrySet()) {
                String nodeId = entry.getKey();
                Map<String, HotKeyMetadata> singleNodeHotKeyMetadata = JSON.parseObject(entry.getValue(),
                        new TypeReference<Map<String, HotKeyMetadata>>() {});
                hotKeyMetadata.put(nodeId, singleNodeHotKeyMetadata);
            }
        }
        log.info("Hotkey metadata:[{}]", JSON.toJSONString(hotKeyMetadata));
        return hotKeyMetadata;
    }

    @Override
    public void register(CacheNodeMetadata cacheNodeMetadata) {
        String nodeId = cacheNodeMetadata.getId();
        String metadata = JSON.toJSONString(cacheNodeMetadata);
        redisCommands.hset(RedisConstants.CACHE_NODE_METADATA_KEY, nodeId, metadata);
    }

    @Override
    public void updateHotKeyMetadata(Map<String, HotKeyMetadata> hotKeyMetadata) {
        String hotKeyJson = JSON.toJSONString(hotKeyMetadata);
        redisCommands.hset(RedisConstants.HOT_KEY_METADATA_KEY, nodeIdentityGenerator.getNodeId(), hotKeyJson);
    }

    @Override
    public HotKeyReplicationMetadata getHotKeyReplicationMetadata(String hotKey) {
        String rawData = redisCommands.hget(RedisConstants.HOT_KEY_REPLICATION, hotKey);
        if (StringUtils.isNotBlank(rawData)) {
            return JSON.parseObject(rawData, HotKeyReplicationMetadata.class);
        } else {
            return null;
        }
    }

    @Override
    public Map<String, HotKeyReplicationMetadata> getAllHotKeyReplicationMetadata() {
        Map<String, String> rawData = redisCommands.hgetall(RedisConstants.HOT_KEY_REPLICATION);
        Map<String, HotKeyReplicationMetadata> hotKeyReplicationMetadata = new HashMap<>();
        if (MapUtils.isNotEmpty(rawData)) {
            for (Map.Entry<String, String> entry : rawData.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                hotKeyReplicationMetadata.put(key, JSON.parseObject(value, HotKeyReplicationMetadata.class));
            }
        }
        log.info("Hotkey replication metadata:[{}]", JSON.toJSONString(hotKeyReplicationMetadata));
        return hotKeyReplicationMetadata;
    }

    @Override
    public void updateHotKeyReplicaNodes(HotKeyReplicationMetadata hotKeyReplicationMetadata) {
        String key = hotKeyReplicationMetadata.getKey();
        String hotKeyReplicationMetadataKey = RedisConstants.HOT_KEY_REPLICATION;

        redisCommands.hset(hotKeyReplicationMetadataKey, key, JSON.toJSONString(hotKeyReplicationMetadata));
    }

    @Override
    public void deleteHotKeyReplicationMetadata(String hotKey) {
        redisCommands.hdel(RedisConstants.HOT_KEY_REPLICATION, hotKey);
    }

    @Override
    public void updateHotKeyWriteVersions(Map<String, Long> hotKeyWriteVersions) {
        if (MapUtils.isEmpty(hotKeyWriteVersions)) {
            return;
        }
        redisCommands.hset(
                RedisConstants.HOT_KEY_WRITE_VERSION,
                nodeIdentityGenerator.getNodeId(),
                JSON.toJSONString(hotKeyWriteVersions));
    }

    @Override
    public Map<String, Long> getAllHotKeyWriteVersions() {
        Map<String, String> rawData = redisCommands.hgetall(RedisConstants.HOT_KEY_WRITE_VERSION);
        Map<String, Long> hotKeyWriteVersions = new HashMap<>();
        if (MapUtils.isEmpty(rawData)) {
            return hotKeyWriteVersions;
        }

        for (String singleNodeWriteVersions : rawData.values()) {
            if (StringUtils.isBlank(singleNodeWriteVersions)) {
                continue;
            }
            Map<String, Long> nodeWriteVersions = JSON.parseObject(
                    singleNodeWriteVersions, new TypeReference<Map<String, Long>>() {});
            if (MapUtils.isEmpty(nodeWriteVersions)) {
                continue;
            }
            nodeWriteVersions.forEach((key, version) -> {
                if (StringUtils.isBlank(key) || version == null) {
                    return;
                }
                hotKeyWriteVersions.merge(key, version, Long::sum);
            });
        }
        log.info("Hotkey write versions:[{}]", JSON.toJSONString(hotKeyWriteVersions));
        return hotKeyWriteVersions;
    }

    @Override
    public void updateCacheNodeRuntimeMetadata(CacheNodeRuntimeMetadata cacheNodeRuntimeMetadata) {
        String jsonString = JSON.toJSONString(cacheNodeRuntimeMetadata);
        redisCommands.hset(RedisConstants.CACHE_NODE_RUNTIME_KEY, cacheNodeRuntimeMetadata.getId(), jsonString);
    }
}
