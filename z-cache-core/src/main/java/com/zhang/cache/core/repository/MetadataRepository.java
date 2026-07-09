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
package com.zhang.cache.core.repository;

import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMetadata;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMigrationMetadata;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeRuntimeMetadata;
import com.zhang.cache.core.metadata.hotkey.entity.HotKeyMetadata;
import com.zhang.cache.core.metadata.hotkey.entity.HotKeyReplicationMetadata;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
public interface MetadataRepository {
    boolean lockForReplication();

    void unlockForReplication();

    Map<String, CacheNodeMetadata> getAllCacheNodeMetadata();

    Map<String, CacheNodeRuntimeMetadata> getAllCacheNodeRuntimeMetadata();

    Map<String, CacheNodeMigrationMetadata> getAllCacheNodeMigrationMetadata();

    Map<String, Map<String, HotKeyMetadata>> getAllHotKeyMetadata();

    void register(CacheNodeMetadata cacheNodeMetadata);

    void updateCacheNodeMigrationMetadata(CacheNodeMigrationMetadata migrationMetadata);

    void deleteCacheNodeMigrationMetadata(String newNodeId);

    void updateHotKeyMetadata(Map<String, HotKeyMetadata> hotKeyMetadata);

    @Nullable
    HotKeyReplicationMetadata getHotKeyReplicationMetadata(String hotKey);

    Map<String, HotKeyReplicationMetadata> getAllHotKeyReplicationMetadata();

    void updateHotKeyReplicaNodes(HotKeyReplicationMetadata hotKeyReplicationMetadata);

    void deleteHotKeyReplicationMetadata(String hotKey);

    void updateHotKeyWriteVersions(Map<String, Long> hotKeyWriteVersions);

    Map<String, Long> getAllHotKeyWriteVersions();

    void updateCacheNodeRuntimeMetadata(CacheNodeRuntimeMetadata cacheNodeRuntimeMetadata);
}
