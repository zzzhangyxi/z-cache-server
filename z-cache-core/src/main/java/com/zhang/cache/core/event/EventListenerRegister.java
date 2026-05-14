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
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author zzzhangyxi
 * @since 2026/5/14
 */
@Component
@Slf4j
public class EventListenerRegister implements ApplicationContextAware, SmartInitializingSingleton {
    private static ApplicationContext context;
    private final Map<Class<? extends BaseEventEntity>, List<EventListener<? extends BaseEventEntity>>> listenerMap = new ConcurrentHashMap<>();

    @Override
    public void setApplicationContext(@Nonnull ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void afterSingletonsInstantiated() {
        Collection<EventPublisher> values = context.getBeansOfType(EventPublisher.class).values();
        System.out.println(values);
        Collection<EventListener> listeners = context.getBeansOfType(EventListener.class).values();
        if (CollectionUtils.isEmpty(listeners)) {
            log.warn("No event listener found.");
            return;
        }
        for (EventListener<?> listener : listeners) {
            Class<?> supportType = listener.supportType();
            listenerMap
                    .computeIfAbsent((Class<? extends BaseEventEntity>) supportType, k -> new CopyOnWriteArrayList<>())
                    .add(listener);
            log.info("Register event listener:{}, support:{}", listener.getClass().getName(), supportType.getName());
        }
        log.info("Event listener register finished, total:{}", listeners.size());
    }

    public List<EventListener<? extends BaseEventEntity>> getListeners(Class<? extends BaseEventEntity> eventClass) {
        return listenerMap.getOrDefault(eventClass, Collections.emptyList());
    }
}