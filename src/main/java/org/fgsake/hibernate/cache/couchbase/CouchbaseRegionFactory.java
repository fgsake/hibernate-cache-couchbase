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

import com.couchbase.client.CouchbaseClient;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.*;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Settings;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CouchbaseRegionFactory implements RegionFactory {
    private static final Logger log = Logger.getLogger(CouchbaseRegionFactory.class);

    public static final String CACHE_HOSTS_PROPERTY = "hibernate.cache.couchbase.hosts";
    public static final String CACHE_BUCKET_PROPERTY = "hibernate.cache.couchbase.bucket";
    public static final String CACHE_PASSWORD_PROPERTY = "hibernate.cache.couchbase.password";
    public static final String CACHE_DEFAULT_EXPIRY_PROPERTY = "hibernate.cache.couchbase.defaultExpiry";

    private ClientWrapper client;
    private int expiry;

    public void start(Settings settings, Properties props) throws CacheException {
        String hosts = props.getProperty(CACHE_HOSTS_PROPERTY, "localhost");
        String bucketName = props.getProperty(CACHE_BUCKET_PROPERTY, "cache");
        String password = props.getProperty(CACHE_PASSWORD_PROPERTY, "");
        expiry = Integer.parseInt(props.getProperty(CACHE_DEFAULT_EXPIRY_PROPERTY, "3600"));

        log.debugf("Starting with hosts: '%s' and bucket: %s", hosts, bucketName);
        try {
            client = new ClientWrapper(new CouchbaseClient(bootstrapUris(hosts), bucketName, password));
        } catch (IOException e) {
            throw new CacheException(e);
        }
    }

    private static List<URI> bootstrapUris(String hosts) {
        List<URI> uris = new ArrayList<URI>();
        for (String host : Arrays.asList(hosts.split("[, ]"))) {
            String hostWithPort = host.contains(":") ? host : host + ":8091";
            uris.add(URI.create("http://" + hostWithPort + "/pools"));
        }
        return uris;
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
        return new CouchbaseEntityRegion(client, metadata, regionName, expiry);
    }

    public NaturalIdRegion buildNaturalIdRegion(String regionName, Properties properties, CacheDataDescription metadata) throws CacheException {
        log.tracef("Building natural ID region %s", regionName);
        return new CouchbaseNaturalIdRegion(client, metadata, regionName, expiry);
    }

    public CollectionRegion buildCollectionRegion(String regionName, Properties properties, CacheDataDescription metadata) throws CacheException {
        log.tracef("Building collection region %s", regionName);
        return new CouchbaseCollectionRegion(client, metadata, regionName, expiry);
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
