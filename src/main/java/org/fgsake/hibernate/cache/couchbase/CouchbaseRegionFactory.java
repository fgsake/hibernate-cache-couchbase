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

import org.fgsake.hibernate.cache.couchbase.internal.*;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.*;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Settings;
import org.jboss.logging.Logger;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CouchbaseRegionFactory implements RegionFactory {
    private static final Logger log = Logger.getLogger(CouchbaseRegionFactory.class);

    /**
     * Fully qualified name of a class that implements {@link MemcachedClientFactory}.
     * It must have a public no-arg constructor. Default is {@link CouchbaseClientFactory}.
     */
    public static final String CACHE_CLIENT_FACTORY_PROPERTY = "hibernate.cache.couchbase.client_factory_class";
    /**
     * Default entry expiration time, in seconds. Default is 3600.
     */
    public static final String CACHE_DEFAULT_EXPIRY_PROPERTY = "hibernate.cache.couchbase.default_expiry";
    /**
     * Whether to automatically upgrade NONSTRICT_READ_WRITE regions to READ_WRITE.
     * Default is false.
     */
    public static final String CACHE_IGNORE_NONSTRICT_PROPERTY = "hibernate.cache.couchbase.ignore_nonstrict";

    public static final String CACHE_SCHEMA_VERSION_PROPERTY = "hibernate.cache.couchbase.schema_version";

    private ClientWrapper client;
    private int expiry;
    private boolean ignoreNonstrict;
    private int schemaVersion;

    public void start(Settings settings, Properties props) throws CacheException {
        schemaVersion = Integer.parseInt(props.getProperty(CACHE_SCHEMA_VERSION_PROPERTY, "-1"));

        String factoryClassName = props.getProperty(CACHE_CLIENT_FACTORY_PROPERTY, "org.fgsake.hibernate.cache.couchbase.internal.CouchbaseClientFactory");

        MemcachedClientFactory factory;
        try {
            Class<?> factoryClass = Class.forName(factoryClassName);
            factory = MemcachedClientFactory.class.cast(factoryClass.getConstructor().newInstance());
        } catch (Exception e) {
            throw new CacheException("Unable to instantiate client factory class " + factoryClassName);
        }

        expiry = Integer.parseInt(props.getProperty(CACHE_DEFAULT_EXPIRY_PROPERTY, "3600"));
        ignoreNonstrict = Boolean.parseBoolean(props.getProperty(CACHE_IGNORE_NONSTRICT_PROPERTY, "false"));

        try {
            client = new ClientWrapper(factory.create(props));
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

    public void stop() {
        log.debug("Shutting down");
        client.shutdown(10, TimeUnit.SECONDS);
    }

    public boolean isMinimalPutsEnabledByDefault() {
        return false;
    }

    public AccessType getDefaultAccessType() {
        return AccessType.READ_WRITE;
    }

    public long nextTimestamp() {
        return Timestamper.nextTimestamp();
    }

    public EntityRegion buildEntityRegion(String regionName, Properties properties, CacheDataDescription metadata) throws CacheException {
        log.tracef("Building entity region %s", regionName);
        return new CouchbaseEntityRegion(client, metadata, regionName, expiry, ignoreNonstrict, schemaVersion);
    }

    public NaturalIdRegion buildNaturalIdRegion(String regionName, Properties properties, CacheDataDescription metadata) throws CacheException {
        log.tracef("Building natural ID region %s", regionName);
        return new CouchbaseNaturalIdRegion(client, metadata, regionName, expiry, ignoreNonstrict, schemaVersion);
    }

    public CollectionRegion buildCollectionRegion(String regionName, Properties properties, CacheDataDescription metadata) throws CacheException {
        log.tracef("Building collection region %s", regionName);
        return new CouchbaseCollectionRegion(client, metadata, regionName, expiry, ignoreNonstrict, schemaVersion);
    }

    public QueryResultsRegion buildQueryResultsRegion(String regionName, Properties properties) throws CacheException {
        log.tracef("Building query results region %s", regionName);
        return new CouchbaseGeneralDataRegion(client, regionName, expiry);
    }

    public TimestampsRegion buildTimestampsRegion(String regionName, Properties properties) throws CacheException {
        log.tracef("Building timestamp region %s", regionName);
        return new CouchbaseGeneralDataRegion(client, regionName, 0);
    }
}
