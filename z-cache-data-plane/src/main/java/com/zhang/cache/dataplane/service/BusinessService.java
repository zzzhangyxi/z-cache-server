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
package com.zhang.cache.dataplane.service;

import com.zhang.cache.core.event.EventPublisher;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMetadata;
import com.zhang.cache.core.metadata.hotkey.HotKeyWriteVersionManager;
import com.zhang.cache.core.repository.BusinessRepository;
import com.zhang.cache.core.hash.HashRouter;
import com.zhang.cache.dataplane.event.entity.ReadKeyEvent;
import com.zhang.cache.dataplane.event.entity.WriteKeyEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
@Service
public class BusinessService {
    @Autowired
    private HashRouter hashRouter;
    @Autowired
    private BusinessRepository businessRepository;
    @Autowired
    private EventPublisher eventPublisher;
    @Autowired
    private HotKeyWriteVersionManager hotKeyWriteVersionManager;

    public String get(String key) {
        return get(key, null);
    }

    public String get(String key, CacheNodeMetadata node) {
        if (node == null) {
            node = hashRouter.route(key);
        }
        eventPublisher.publishEvent(new ReadKeyEvent(key));
        return businessRepository.get(key, node);
    }

    public void set(String key, String value) {
        set(key, value, null);
    }

    public void set(String key, String value, CacheNodeMetadata node) {
        boolean specifyNode = node != null;
        if (!specifyNode) {
            List<CacheNodeMetadata> writeNodes = hashRouter.writeRoute(key);
            for (CacheNodeMetadata writeNode : writeNodes) {
                businessRepository.set(key, value, writeNode);
            }
        } else {
            businessRepository.set(key, value, node);
        }
        if (!specifyNode) {
            hotKeyWriteVersionManager.markWritten(key);
            eventPublisher.publishEvent(new WriteKeyEvent(key));
        }
    }

    public void setEx(String key, String value, int seconds) {
        setEx(key, value, seconds, null);
    }

    public void setEx(String key, String value, int seconds, CacheNodeMetadata node) {
        boolean specifyNode = node != null;
        if (!specifyNode) {
            List<CacheNodeMetadata> writeNodes = hashRouter.writeRoute(key);
            for (CacheNodeMetadata writeNode : writeNodes) {
                businessRepository.setEx(key, value, seconds, writeNode);
            }
        } else {
            businessRepository.setEx(key, value, seconds, node);
        }
        if (!specifyNode) {
            hotKeyWriteVersionManager.markWritten(key);
            eventPublisher.publishEvent(new WriteKeyEvent(key));
        }
    }

    public void delete(String key) {
        delete(key, null);
    }

    public void delete(String key, CacheNodeMetadata node) {
        boolean specifyNode = node != null;
        if (!specifyNode) {
            List<CacheNodeMetadata> writeNodes = hashRouter.writeRoute(key);
            for (CacheNodeMetadata writeNode : writeNodes) {
                businessRepository.del(key, writeNode);
            }
        } else {
            businessRepository.del(key, node);
        }
        if (!specifyNode) {
            hotKeyWriteVersionManager.markWritten(key);
            eventPublisher.publishEvent(new WriteKeyEvent(key));
        }
    }
}
