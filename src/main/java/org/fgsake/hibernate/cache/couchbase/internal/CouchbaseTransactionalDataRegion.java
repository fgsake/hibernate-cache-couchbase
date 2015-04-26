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

import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.TransactionalDataRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.RegionAccessStrategy;
import org.hibernate.cache.spi.access.SoftLock;
import org.jboss.logging.Logger;

import java.util.Comparator;

public class CouchbaseTransactionalDataRegion extends CouchbaseRegion implements TransactionalDataRegion {
    private static final Logger log = Logger.getLogger(CouchbaseTransactionalDataRegion.class);

    protected final CacheDataDescription description;
    protected final Comparator versionComparator;

    public CouchbaseTransactionalDataRegion(ClientWrapper client, CacheDataDescription description, String name, int expiry) {
        super(client, name, expiry);
        this.description = description;
        this.versionComparator = description.getVersionComparator();
    }

    public boolean isTransactionAware() {
        return false;
    }

    public CacheDataDescription getCacheDataDescription() {
        return description;
    }

    public class AccessStrategy implements RegionAccessStrategy {
        protected final AccessType accessType;

        public AccessStrategy(AccessType accessType) {
            this.accessType = accessType;
        }

        public Object get(Object key, long txTimestamp) throws CacheException {
            String keyStr = keyStrFor(key);
            log.debugf("Get %s", keyStr);
            Object value = client.get(keyStr);

            if (value instanceof CacheData) {
                CacheData data = (CacheData) value;
                if (data.getTimestamp() > txTimestamp) {
                    return null;
                }
                return data.getValue();
            }
            return null;
        }

        public boolean putFromLoad(Object key, Object value, long txTimestamp, Object version) throws CacheException {
            return putFromLoad(key, value, txTimestamp, version, false);
        }

        public boolean putFromLoad(Object key, Object value, long txTimestamp, Object version, boolean minimalPutOverride) throws CacheException {
            String keyStr = keyStrFor(key);
            log.debugf("Put from load %s", key);

            CASValue<Object> rsp = client.gets(keyStr);
            if (rsp == null) {
                client.add(keyStr, expiry, new CacheData(txTimestamp, version, value));
                return true;
            }

            Object v = rsp.getValue();
            if (v instanceof CacheItem && ((CacheItem) v).writable(txTimestamp, version, versionComparator)) {
                client.asyncCAS(keyStr, rsp.getCas(), expiry, new CacheData(txTimestamp, version, value));
                return true;
            }
            return false;
        }

        public SoftLock lockItem(Object key, Object version) throws CacheException {
            String keyStr = keyStrFor(key);
            log.debugf("Lock %s", keyStr);

            if (accessType == AccessType.NONSTRICT_READ_WRITE) {
                return null;
            } else if (accessType == AccessType.READ_ONLY) {
                throw new UnsupportedOperationException("Can't write to a readonly object");
            }

            while (true) {
                CASValue<Object> rsp = client.gets(keyStr);
                if (rsp == null) {
                    if (client.add(keyStr, 0, new CacheLock(version, nextTimestamp() + getTimeout())).getStatus().isSuccess()) {
                        return null;
                    }
                    continue;
                }

                CacheItem item = (CacheItem) rsp.getValue();
                if (item instanceof CacheLock) {
                    ((CacheLock) item).lock(nextTimestamp() + getTimeout());
                } else {
                    item = new CacheLock(version, nextTimestamp() + getTimeout());
                }

                switch (client.cas(keyStr, rsp.getCas(), 0, item)) {
                case OK:
                    return null;
                case NOT_FOUND:
                    if (client.add(keyStr, 0, new CacheLock(version, nextTimestamp() + getTimeout())).getStatus().isSuccess()) {
                        return null;
                    }
                    break;
                case EXISTS:
                    break;
                }
            }
        }

        public SoftLock lockRegion() throws CacheException {
            return null;
        }

        public void unlockItem(Object key, SoftLock lock) throws CacheException {
            String keyStr = keyStrFor(key);
            log.debugf("Unlock %s", keyStr);

            if (accessType == AccessType.NONSTRICT_READ_WRITE) {
                client.delete(keyStrFor(key));
                return;
            }

            while (true) {
                CASValue<Object> rsp = client.gets(keyStr);
                if (rsp == null || rsp.getValue() == null) {
                    return;
                }

                CacheItem item = (CacheItem) rsp.getValue();
                if (item instanceof CacheLock) {
                    ((CacheLock) item).unlock(nextTimestamp());
                } else {
                    return;
                }
                switch (client.cas(keyStr, rsp.getCas(), 0, item)) {
                case OK:
                    return;
                case NOT_FOUND:
                    return;
                case EXISTS:
                    break;
                }
            }
        }

        public void unlockRegion(SoftLock lock) throws CacheException {
        }

        public void remove(Object key) throws CacheException {
        }

        public void removeAll() throws CacheException {
            throw new CacheException("removeAll not supported");
        }

        public void evict(Object key) throws CacheException {
            String keyStr = keyStrFor(key);
            log.debugf("Evict %s", keyStr);
            client.delete(keyStr);
        }

        public void evictAll() throws CacheException {
            throw new CacheException("evictAll not supported");
        }

        protected boolean afterInsert(Object key, Object value, Object version) throws CacheException {
            String keyStr = keyStrFor(key);
            log.debugf("Insert %s", keyStr);
            client.add(keyStr, expiry, new CacheData(nextTimestamp(), version, value));
            return true;
        }

        protected boolean afterUpdate(Object key, Object value, Object version) throws CacheException {
            String keyStr = keyStrFor(key);
            log.debugf("Update %s", keyStr);

            if (accessType == AccessType.READ_ONLY) {
                throw new UnsupportedOperationException("Can't write to a readonly object");
            } else if (accessType == AccessType.NONSTRICT_READ_WRITE) {
                client.set(keyStr, expiry, new CacheData(0, version, value));
            }

            while (true) {
                CASValue<Object> rsp = client.gets(keyStr);
                if (rsp == null) {
                    return false;
                }

                CacheItem item = (CacheItem) rsp.getValue();
                if (item instanceof CacheLock) {
                    CacheLock lock = (CacheLock) item;
                    if (lock.unlock(nextTimestamp())) {
                        if (client.cas(keyStr, rsp.getCas(), expiry, new CacheData(nextTimestamp(), version, value)) == CASResponse.OK) {
                            return true;
                        }
                    } else if (client.cas(keyStr, rsp.getCas(), 0, lock) == CASResponse.OK) {
                        return true;
                    }
                } else {
                    return false;
                }
            }
        }
    }
}
