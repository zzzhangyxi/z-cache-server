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
package com.zhang.cache.interfaces.http;

import com.zhang.cache.controlplane.service.MetadataRegisterService;
import com.zhang.cache.interfaces.http.dto.MetadataRegisterRequestDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
@RestController
@RequestMapping("/metadata")
public class MetadataController {
    @Autowired
    private MetadataRegisterService metadataRegisterService;

    @PostMapping("/register")
    public String register(@RequestBody MetadataRegisterRequestDTO request) {
        metadataRegisterService.register(request.getId(), request.getIp(), request.getPort());
        return "success";
    }
}
