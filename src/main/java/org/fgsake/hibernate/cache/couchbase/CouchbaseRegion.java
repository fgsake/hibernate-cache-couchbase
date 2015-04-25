/*
 * Copyright 2015 For Goodness Sake, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fgsake.hibernate.cache.couchbase;

import org.apache.commons.codec.digest.DigestUtils;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.Region;
import org.hibernate.internal.util.SerializationHelper;

import java.util.Collections;
import java.util.Map;

public class CouchbaseRegion implements Region {
    protected final ClientWrapper client;
    protected final String name;
    protected final int expiry;

    public CouchbaseRegion(ClientWrapper client, String name, int expiry) {
        this.client = client;
        this.name = name;
        this.expiry = expiry;
    }

    public String getName() {
        return name;
    }

    public void destroy() throws CacheException {
    }

    public boolean contains(Object key) {
        return false;
    }

    public long getSizeInMemory() {
        return -1;
    }

    public long getElementCountInMemory() {
        return -1;
    }

    public long getElementCountOnDisk() {
        return -1;
    }

    public Map toMap() {
        return Collections.EMPTY_MAP;
    }

    public long nextTimestamp() {
        return Timestamper.nextTimestamp();
    }

    public int getTimeout() {
        return 30000000;
    }

    protected String keyStrFor(Object key) {
        String keyStr;
        if (key instanceof QueryKey) {
            // Can't use toString, QueryKey.customTransformer shows up wrong in toString
            keyStr = DigestUtils.md5Hex(SerializationHelper.serialize((QueryKey) key));
        } else {
            keyStr = key.toString();
            if (name.length() + 1 + keyStr.length() > 250) {
                keyStr = DigestUtils.md5Hex(keyStr);
            }
        }

        return new StringBuilder(name.length() + 1 + keyStr.length())
                .append(name).append(":").append(keyStr)
                .toString();
    }
}
