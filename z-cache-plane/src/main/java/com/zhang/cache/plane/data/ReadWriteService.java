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
package com.zhang.cache.plane.data;

import com.zhang.cache.core.metadata.cachenode.CacheNodeMetadata;
import com.zhang.cache.core.repository.ReadWriteRepository;
import com.zhang.cache.plane.control.RoutingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
@Service
public class ReadWriteService {
    @Autowired
    private RoutingService routingService;
    @Autowired
    private ReadWriteRepository readWriteRepository;

    public String get(String key) {
        CacheNodeMetadata routeMetadata = routingService.route(key);
        return readWriteRepository.get(key, routeMetadata);
    }

    public void set(String key, String value) {
        CacheNodeMetadata routeMetadata = routingService.route(key);
        readWriteRepository.set(key, value, routeMetadata);
    }

    public void setEx(String key, String value, int seconds) {
        CacheNodeMetadata routeMetadata = routingService.route(key);
        readWriteRepository.setEx(key, value, seconds, routeMetadata);
    }

    public void delete(String key) {
        CacheNodeMetadata routeMetadata = routingService.route(key);
        readWriteRepository.del(key, routeMetadata);
    }
}
