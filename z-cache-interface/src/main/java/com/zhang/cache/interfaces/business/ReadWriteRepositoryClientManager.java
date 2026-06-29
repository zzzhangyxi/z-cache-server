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

import com.zhang.cache.core.exception.ConnectionException;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMetadata;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.security.InvalidParameterException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
@Component
@Slf4j
public class ReadWriteRepositoryClientManager {
    private static final Map<String, StatefulRedisConnection<String, String>> CONNECTIONS = new ConcurrentHashMap<>();
    private static final RedisClient REDIS = RedisClient.create();

    @Value("${business.password}")
    private String password;

    public RedisCommands<String, String> getConnection(CacheNodeMetadata metadata) {
        if (metadata == null) {
            log.error("metadata is null");
            throw new InvalidParameterException("metadata is null");
        }

        String nodeId = metadata.getId();
        StatefulRedisConnection<String, String> connection = CONNECTIONS.computeIfAbsent(nodeId, k -> createConnection(metadata));
        return connection.sync();
    }

    private StatefulRedisConnection<String, String> createConnection(CacheNodeMetadata metadata) {
        RedisURI redisURI = RedisURI.builder()
                .withHost(metadata.getIp())
                .withPort(metadata.getPort())
                .withPassword(password.toCharArray())
                .build();
        try {
            StatefulRedisConnection<String, String> connection = REDIS.connect(redisURI);
            log.info("Connect to {}:{}", redisURI.getHost(), redisURI.getPort());
            return connection;
        } catch (Exception e) {
            log.error("Connect to {}:{} failed", redisURI.getHost(), redisURI.getPort(), e);
            throw new ConnectionException("Connect to " + redisURI.getHost() + ":" + redisURI.getPort() + " failed");
        }
    }
}
