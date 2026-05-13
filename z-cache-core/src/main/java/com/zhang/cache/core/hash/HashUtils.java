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
package com.zhang.cache.core.hash;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

/**
 * @author zzzhangyxi
 * @since 2026/5/13
 */
public class HashUtils {
    private HashUtils() {}

    public static long hash(String key) {
        // after hashing, we got a signed value. but we need an unsigned value to build the hash ring.
        int intHashValue = Hashing.murmur3_32_fixed()
                .hashString(key, StandardCharsets.UTF_8)
                .asInt();
        return Integer.toUnsignedLong(intHashValue);
    }
}
