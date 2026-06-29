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
package com.zhang.cache.core.identity;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

/**
 * @author zzzhangyxi
 * @since 2026/6/27
 */
@Component
@Slf4j
public class NodeIdentityGenerator {
    private static final String NODE_ID_KEY = "node.id";

    @Getter
    private String nodeId;

    @Value("${node-id.file-path}")
    private String filePath;

    @PostConstruct
    private void init() throws IOException {
        Path path = Paths.get(filePath);
        Properties properties = new Properties();
        try {
            if (Files.exists(path)) {
                try (InputStream in = Files.newInputStream(path)) {
                    properties.load(in);
                }
                String id = properties.getProperty(NODE_ID_KEY);
                if (StringUtils.isNotBlank(id)) {
                    nodeId = id;
                }
            } else {
                Files.createFile(path);
                nodeId = UUID.randomUUID().toString();
                properties.setProperty(NODE_ID_KEY, nodeId);
                try (OutputStream out = Files.newOutputStream(path)) {
                    properties.store(out, "Node configuration");
                }
            }
        } catch (IOException e) {
            log.error("failed to load node identity file", e);
            throw e;
        }
    }
}
