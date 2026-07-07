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
package com.zhang.cache.interfaces.metadata.lua;

/**
 * @author zzzhangyxi
 * @since 2026/5/16
 */
@SuppressWarnings("all")
public class LuaScripts {
    public static final String LOCK_FOR_REPLICATION_SCRIPT =
            "local result = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) "
                    + "if result then "
                    + "   return 1 "
                    + "end "
                    + "return 0";

    public static final String UNLOCK_FOR_REPLICATION_SCRIPT =
            "local owner = redis.call('GET', KEYS[1]) "
                    + "if owner == ARGV[1] then "
                    + "   return redis.call('DEL', KEYS[1]) "
                    + "end "
                    + "return 0";
}
