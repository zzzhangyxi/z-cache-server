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
import io.lettuce.core.api.sync.RedisCommands;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

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

    private RedisCommands<String, String> getConnection(CacheNodeMetadata metadata) {
        return businessRepositoryClientManager.getConnection(metadata);
    }

    private String wrapKey(String key) {
        return RedisConstants.BUSINESS_DATA_KEY_PREFIX + key;
    }
}
