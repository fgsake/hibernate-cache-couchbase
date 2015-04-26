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

package org.fgsake.hibernate.cache.couchbase.internal;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.TimestampsRegion;
import org.jboss.logging.Logger;

public class CouchbaseGeneralDataRegion extends CouchbaseRegion implements QueryResultsRegion, TimestampsRegion {
    private static final Logger log = Logger.getLogger(CouchbaseGeneralDataRegion.class);

    public CouchbaseGeneralDataRegion(ClientWrapper client, String name, int expiry) {
        super(client, name, expiry);
    }

    public Object get(Object key) throws CacheException {
        String keyStr = keyStrFor(key);
        log.debugf("Get %s", keyStr);
        return client.get(keyStr);
    }

    public void put(Object key, Object value) throws CacheException {
        String keyStr = keyStrFor(key);
        log.debugf("Put %s", keyStr);
        client.set(keyStr, expiry, value);
    }

    public void evict(Object key) throws CacheException {
        String keyStr = keyStrFor(key);
        log.debugf("Evict %s", keyStr);
        client.delete(keyStr);
    }

    public void evictAll() throws CacheException {
        throw new CacheException("evictAll not supported");
    }
}
