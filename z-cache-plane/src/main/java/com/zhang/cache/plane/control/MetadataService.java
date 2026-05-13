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
package com.zhang.cache.plane.control;

import com.zhang.cache.core.metadata.cachenode.CacheNodeMetadata;
import com.zhang.cache.core.metadata.cachenode.CacheNodeStatus;
import com.zhang.cache.core.repository.MetadataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
@Service
public class MetadataService {
    @Autowired
    private MetadataRepository metadataRepository;

    public void register(String id, String ip, Integer port) {
        CacheNodeMetadata cacheNodeMetadata = CacheNodeMetadata.builder()
                .id(id)
                .ip(ip)
                .port(port == null ? 6379 : port)
                .status(CacheNodeStatus.REGISTERING)
                .startupTimestamp(System.currentTimeMillis())
                .version(1L)
                .build();
        metadataRepository.register(cacheNodeMetadata);
        // TODO 数据复制逻辑
        // initNodeData();
    }
}
