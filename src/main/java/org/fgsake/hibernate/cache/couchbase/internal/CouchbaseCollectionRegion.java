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
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cache.spi.access.CollectionRegionAccessStrategy;

public class CouchbaseCollectionRegion extends CouchbaseTransactionalDataRegion implements CollectionRegion {
    public CouchbaseCollectionRegion(ClientWrapper client, CacheDataDescription description, String name, int expiry) {
        super(client, description, name, expiry);
    }

    public CollectionRegionAccessStrategy buildAccessStrategy(AccessType accessType) throws CacheException {
        if (accessType == AccessType.TRANSACTIONAL) {
            throw new CacheException("Access type " + accessType + " isn't supported");
        }
        return new AccessStrategy(accessType);
    }

    public class AccessStrategy extends CouchbaseTransactionalDataRegion.AccessStrategy implements CollectionRegionAccessStrategy {
        public AccessStrategy(AccessType accessType) {
            super(accessType);
        }

        public CollectionRegion getRegion() {
            return CouchbaseCollectionRegion.this;
        }
    }
}
