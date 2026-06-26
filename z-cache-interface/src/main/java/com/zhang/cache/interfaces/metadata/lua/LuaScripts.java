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
    public static final String HOT_KEY_METADATA_UPDATE_SCRIPT =
            "local old = redis.call('HGET', KEYS[1], ARGV[2])" +
                    "local oldTs = 0" +
                    "if old then" +
                    "    local sep = string.find(old, \"|\")" +
                    "    oldTs = tonumber(string.sub(old, sep + 1))" +
                    "end" +
                    "if (not old) or (tonumber(ARGV[1]) > oldTs) then" +
                    "    redis.call(" +
                    "        'HSET'," +
                    "        KEYS[1]," +
                    "        ARGV[2]," +
                    "        ARGV[3] .. \"|\" .. ARGV[1]" +
                    "    )" +
                    "    return 1" +
                    "end" +
                    "return 0";

    public static final String HOT_KEY_REPLICATION_METADATA_UPDATE_SCRIPT =
            "local oldTs = redis.call('HGET', KEYS[1], 'last_operation_timestamp') " +
                    "if (not oldTs) or (tonumber(ARGV[1]) > tonumber(oldTs)) then " +
                    "   redis.call('HSET', KEYS[1], " +
                    "       'key', ARGV[2], " +
                    "       'nodes', ARGV[3], " +
                    "       'last_operation_timestamp', ARGV[1]) " +
                    "   return 1 " +
                    "end " +
                    "return 0";

    public static final String LOCK_FOR_REPLICATION_SCRIPT =
            "local status = redis.call('HGET', KEYS[1], 'status') "
                    + "if not status then "
                    + "   return 0 "
                    + "end "
                    + "if status ~= ARGV[1] then "
                    + "   return 0 "
                    + "end "
                    + "local result = redis.call("
                    + "   'SET',"
                    + "   KEYS[2],"
                    + "   ARGV[2],"
                    + "   'NX',"
                    + "   'PX',"
                    + "   ARGV[3]"
                    + ") "
                    + "if result then "
                    + "   return 1 "
                    + "end "
                    + "return 0";

    public static final String UNLOCK_FOR_REPLICATION_SCRIPT =
            "if redis.call('GET', KEYS[1]) == ARGV[1] then " +
                    "   return redis.call('DEL', KEYS[1]) " +
                    "end " +
                    "return 0";
}
