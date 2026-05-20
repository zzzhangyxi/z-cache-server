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

import com.alibaba.fastjson.JSON;
import com.zhang.cache.core.exception.InvalidParamException;
import com.zhang.cache.interfaces.http.dto.BusinessWriteRequestDTO;
import com.zhang.cache.core.service.ReadWriteService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
@RestController
@RequestMapping("/business")
@Slf4j
public class BusinessController {
    @Autowired
    private ReadWriteService readWriteService;

    @GetMapping("/read")
    public String read(@RequestParam String key) {
        return readWriteService.get(key);
    }

    @PostMapping("/write")
    public String write(@RequestBody BusinessWriteRequestDTO request) {
        validateParam(request);
        readWriteService.set(request.getKey(), request.getValue());
        return "success";
    }

    @DeleteMapping("/delete")
    public String delete(@RequestParam String key) {
        readWriteService.delete(key);
        return "success";
    }

    private void validateParam(BusinessWriteRequestDTO request) {
        boolean valid = StringUtils.isNotBlank(request.getKey()) && StringUtils.isNotBlank(request.getValue());
        if (!valid) {
            log.error("Invalid request param:[{}]", JSON.toJSONString(request));
            throw new InvalidParamException("Invalid request param");
        }
    }
}
