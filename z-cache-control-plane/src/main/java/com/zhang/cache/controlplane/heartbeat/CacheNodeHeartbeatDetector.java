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
package com.zhang.cache.controlplane.heartbeat;

import com.alibaba.fastjson.JSON;
import com.zhang.cache.controlplane.constant.CacheNodeRuntimeConstants;
import com.zhang.cache.controlplane.threadpool.ControlPlaneThreadPool;
import com.zhang.cache.core.event.EventPublisher;
import com.zhang.cache.core.event.entity.CacheNodeMetadataRefreshEvent;
import com.zhang.cache.core.metadata.cachenode.CacheNodeMetadataManager;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeMetadata;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeRuntimeMetadata;
import com.zhang.cache.core.metadata.cachenode.entity.CacheNodeStatus;
import com.zhang.cache.core.repository.BusinessRepository;
import com.zhang.cache.core.repository.MetadataRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author zzzhangyxi
 * @since 2026/6/29
 */
@Component
@Slf4j
public class CacheNodeHeartbeatDetector {
    @Autowired
    private MetadataRepository metadataRepository;
    @Autowired
    private BusinessRepository businessRepository;
    @Autowired
    private CacheNodeMetadataManager cacheNodeMetadataManager;
    @Autowired
    private EventPublisher eventPublisher;

    @Scheduled(fixedRate = 1000)
    public void heartbeat() {
        long now = System.currentTimeMillis();

        // heartbeat detect
        Map<String, CacheNodeMetadata> cacheNodeMetadata = metadataRepository.getAllCacheNodeMetadata();
        log.info("Start to detect heartbeat of each cache node, node metadata list:[{}]", JSON.toJSONString(cacheNodeMetadata));

        Map<String, CacheNodeRuntimeMetadata> cacheNodeRuntimeMetadata = metadataRepository.getAllCacheNodeRuntimeMetadata();

        List<CompletableFuture<CacheNodeRuntimeMetadata>> taskList = new ArrayList<>();
        for (Map.Entry<String, CacheNodeMetadata> entry : cacheNodeMetadata.entrySet()) {
            String nodeId = entry.getKey();
            CacheNodeMetadata node = entry.getValue();

            CacheNodeRuntimeMetadata runtime = cacheNodeRuntimeMetadata.get(nodeId);
            if (runtime == null || runtime.needDetectHeartbeat()) {
                CompletableFuture<CacheNodeRuntimeMetadata> task = CompletableFuture.supplyAsync(() -> {
                    log.info("Start to detect heartbeat of node [{}]", nodeId);
                    return detectHeartbeat(nodeId, node, runtime, now);
                }, ControlPlaneThreadPool.getHeartbeatExecutor());
                taskList.add(task);
            }
        }
        if (CollectionUtils.isNotEmpty(taskList)) {
            try {
                CompletableFuture.allOf(taskList.toArray(new CompletableFuture[0])).join();
            } catch (Exception e) {
                log.error("Detect failed", e);
            }

            // refresh runtime metadata
            Map<String, CacheNodeRuntimeMetadata> detectResult = new HashMap<>();
            for (CompletableFuture<CacheNodeRuntimeMetadata> task : taskList) {
                try {
                    CacheNodeRuntimeMetadata runtime = task.get();
                    detectResult.put(runtime.getId(), runtime);
                    metadataRepository.updateCacheNodeRuntimeMetadata(runtime);
                } catch (InterruptedException | ExecutionException e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    log.error("Get detect task failed", e);
                }
            }
            cacheNodeMetadataManager.setCacheNodeRuntimeMetadata(detectResult);
            eventPublisher.publishEvent(new CacheNodeMetadataRefreshEvent());
        }
    }

    private CacheNodeRuntimeMetadata detectHeartbeat(String nodeId, CacheNodeMetadata node,CacheNodeRuntimeMetadata runtime, long timestamp) {
        boolean success;
        try {
            String heartbeat = businessRepository.ping(node);
            success = "PONG".equals(heartbeat);
        } catch (Exception e) {
            success = false;
        }

        if (success) {
            CacheNodeRuntimeMetadata result = new CacheNodeRuntimeMetadata();
            result.setId(nodeId);
            result.setStatus(CacheNodeStatus.ONLINE);
            result.setLastHeartbeatTimestamp(timestamp);
            result.setFailedTimes(0);
            return result;
        } else {
            return buildFailedMetadata(nodeId, runtime, timestamp);
        }
    }

    private static CacheNodeRuntimeMetadata buildFailedMetadata(String nodeId, CacheNodeRuntimeMetadata runtime, long timestamp) {
        Integer failedTimes = runtime == null ? null : runtime.getFailedTimes();
        int currentFailTimes = (failedTimes == null ? 0 : failedTimes) + 1;
        CacheNodeStatus status;
        if (currentFailTimes >= CacheNodeRuntimeConstants.HEARTBEAT_OFFLINE_THRESHOLD) {
            currentFailTimes = CacheNodeRuntimeConstants.HEARTBEAT_OFFLINE_THRESHOLD;
            status = CacheNodeStatus.OFFLINE;
        } else {
            status = CacheNodeStatus.SUSPECTED_OFFLINE;
        }

        CacheNodeRuntimeMetadata result = new CacheNodeRuntimeMetadata();
        result.setId(nodeId);
        result.setStatus(status);
        result.setFailedTimes(currentFailTimes);
        result.setLastHeartbeatTimestamp(timestamp);
        return result;
    }
}
