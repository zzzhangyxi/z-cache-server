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
package com.zhang.cache.interfaces.business;

import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMetadata;
import com.zhang.cache.core.repository.BusinessRepository;
import com.zhang.cache.interfaces.RedisConstants;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.sync.RedisCommands;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.HashSet;
import java.util.Set;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
@Repository
public class BusinessRepositoryImpl implements BusinessRepository {
    @Autowired
    private BusinessRepositoryClientManager businessRepositoryClientManager;

    @Override
    public String get(String key, CacheNodeMetadata metadata) {
        return getConnection(metadata).get(wrapKey(key));
    }

    @Override
    public void set(String key, String value, CacheNodeMetadata metadata) {
        getConnection(metadata).set(wrapKey(key), value);
    }

    @Override
    public void setEx(String key, String value, int seconds, CacheNodeMetadata metadata) {
        getConnection(metadata).setex(wrapKey(key), seconds, value);
    }

    @Override
    public void del(String key, CacheNodeMetadata metadata) {
        getConnection(metadata).del(wrapKey(key));
    }

    @Override
    public Set<String> scanKeys(CacheNodeMetadata metadata) {
        RedisCommands<String, String> connection = getConnection(metadata);
        Set<String> keys = new HashSet<>();
        ScanArgs scanArgs = ScanArgs.Builder
                .matches(RedisConstants.BUSINESS_DATA_KEY_PREFIX + "*")
                .limit(1000);

        ScanCursor scanCursor = ScanCursor.INITIAL;
        do {
            KeyScanCursor<String> keyScanCursor = connection.scan(scanCursor, scanArgs);
            for (String storageKey : keyScanCursor.getKeys()) {
                keys.add(unwrapKey(storageKey));
            }
            scanCursor = keyScanCursor;
        } while (!scanCursor.isFinished());

        return keys;
    }

    @Override
    public Long ttl(String key, CacheNodeMetadata metadata) {
        return getConnection(metadata).ttl(wrapKey(key));
    }

    @Override
    public String ping(CacheNodeMetadata node) {
        return getConnection(node).ping();
    }

    private RedisCommands<String, String> getConnection(CacheNodeMetadata metadata) {
        return businessRepositoryClientManager.getConnection(metadata);
    }

    private String wrapKey(String key) {
        return RedisConstants.BUSINESS_DATA_KEY_PREFIX + key;
    }

    private String unwrapKey(String storageKey) {
        if (storageKey == null || !storageKey.startsWith(RedisConstants.BUSINESS_DATA_KEY_PREFIX)) {
            return storageKey;
        }
        return storageKey.substring(RedisConstants.BUSINESS_DATA_KEY_PREFIX.length());
    }
}
