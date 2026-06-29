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

/**
 * Contains single string value read and write command only.
 * The main purpose of this project is to optimize performance, instead of reimplementing a new RedisTemplate
 *
 * @author zzzhangyxi
 * @since 2026/5/13
 */
public interface BusinessRepository {
    /**
     * Read a single string value.
     * @param key data key
     * @param metadata cache node
     * @return single string value
     */
    String get(String key, CacheNodeMetadata metadata);

    /**
     * Write value, including insert and update.
     * @param key data key
     * @param metadata cache node
     * @param value new value
     */
    void set(String key, String value, CacheNodeMetadata metadata);

    /**
     * Write value with expired time. Write value and set expired time are atomic operations.
     * @param key data key
     * @param value new value
     * @param seconds expire time
     * @param metadata cache node
     */
    void setEx(String key, String value, int seconds, CacheNodeMetadata metadata);

    /**
     * Delete key and data value.
     * @param key data key
     * @param metadata cache node
     */
    void del(String key, CacheNodeMetadata metadata);
}
