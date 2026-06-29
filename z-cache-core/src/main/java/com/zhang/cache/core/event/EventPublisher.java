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
package com.zhang.cache.core.event;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * @author zzzhangyxi
 * @since 2026/5/14
 */
@Component
@Slf4j
public class EventPublisher {
    private static final Map<EventType, Executor> EXECUTORS = new ConcurrentHashMap<>();

    @Autowired
    private EventListenerRegister eventListenerRegister;

    public static void registerExecutor(EventType eventType, Executor executor) {
        // should be null
        Executor oldExecutor = EXECUTORS.put(eventType, executor);
        if (oldExecutor != null) {
            log.error("Same type of executor has been registered: {}", eventType);
            throw new IllegalStateException("Same type of executor has been registered: " + eventType);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends BaseEventEntity> void publishEvent(T event) {
        if (event == null) {
            return;
        }

        List<EventListener<? extends BaseEventEntity>> listeners = eventListenerRegister.getListeners(event.getClass());
        if (CollectionUtils.isEmpty(listeners)) {
            return;
        }

        String eventClass = event.getClass().getName();

        Executor executor = EXECUTORS.get(event.getEventType());
        for (EventListener<? extends BaseEventEntity> listener : listeners) {
            EventListener<T> typedListener = (EventListener<T>) listener;
            String listenerClass = listener.getClass().getName();

            if (executor == null) {
                // Not use thread pool to execute this task.
                try {
                    typedListener.onEvent(event);
                } catch (Exception e) {
                    log.error("Event execute failed. event:{} listener:{}", eventClass, listenerClass, e);
                }
            } else {
                CompletableFuture
                        .runAsync(() -> typedListener.onEvent(event), executor)
                        .exceptionally(ex -> {
                            log.error("Event execute failed. event:{} listener:{}", eventClass, listenerClass, ex);
                            return null;
                        });
            }
        }
    }
}
