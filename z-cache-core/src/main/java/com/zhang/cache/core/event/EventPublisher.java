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

import com.zhang.cache.core.exception.EventException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author zzzhangyxi
 * @since 2026/5/14
 */
@Component
@Slf4j
public class EventPublisher {
    @Autowired
    private EventListenerRegister eventListenerRegister;

    private static final Executor RELIABLE_EVENT_POOL = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors() * 4,
            Runtime.getRuntime().availableProcessors() * 30,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000),
            new ThreadPoolExecutor.CallerRunsPolicy());
    private static final Executor UNRELIABLE_EVENT_POOL = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors() * 20,
            Runtime.getRuntime().availableProcessors() * 50,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000),
            new ThreadPoolExecutor.CallerRunsPolicy());

    @SuppressWarnings("unchecked")
    public <T extends BaseEventEntity> void publishEvent(T event) {
        if (event == null) {
            return;
        }

        List<EventListener<? extends BaseEventEntity>> listeners = eventListenerRegister.getListeners(event.getClass());
        if (CollectionUtils.isEmpty(listeners)) {
            return;
        }

        Executor executor = event.reliable() ? RELIABLE_EVENT_POOL : UNRELIABLE_EVENT_POOL;
        for (EventListener<? extends BaseEventEntity> listener : listeners) {
            EventListener<T> typedListener = (EventListener<T>) listener;
            CompletableFuture
                    .runAsync(() -> typedListener.onEvent(event), executor)
                    .exceptionally(ex -> {
                        String eventClass = event.getClass().getName();
                        String listenerClass = listener.getClass().getName();
                        log.error("Event execute failed. event:{} listener:{}", eventClass, listenerClass, ex);
                        return null;
                    });
        }
    }
}
