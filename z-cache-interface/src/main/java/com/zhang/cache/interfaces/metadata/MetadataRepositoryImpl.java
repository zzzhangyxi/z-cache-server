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
import com.zhang.cache.core.constant.HotKeyConstants;
import com.zhang.cache.core.metadata.cachenode.CacheNodeMetadata;
import com.zhang.cache.core.metadata.hotkey.HotKeyMetadata;
import com.zhang.cache.core.repository.MetadataRepository;
import com.zhang.cache.interfaces.RedisConstants;
import com.zhang.cache.interfaces.metadata.lua.LuaScripts;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
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
    public void updateHotKeyMetadata(HotKeyMetadata hotKeyMetadata) {
        String key = hotKeyMetadata.getKey();
        String hotKeyMetadataKey = HotKeyConstants.HOT_KEY_PREFIX + key;

        // use the lua script to update metadata atomically, avoid new version data being covered by old version data.
        redisCommands.eval(
                LuaScripts.HOT_KEY_METADATA_UPDATE_SCRIPT,
                ScriptOutputType.INTEGER,
                new String[]{hotKeyMetadataKey},
                String.valueOf(hotKeyMetadata.getLastOperationTimestamp()),
                key,
                hotKeyMetadata.getStatus().name());
    }
}
