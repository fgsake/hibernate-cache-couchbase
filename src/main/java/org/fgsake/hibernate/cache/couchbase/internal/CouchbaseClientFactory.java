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

import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.MemcachedClient;
import org.fgsake.hibernate.cache.couchbase.MemcachedClientFactory;
import org.jboss.logging.Logger;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class CouchbaseClientFactory implements MemcachedClientFactory {
    private static final Logger log = Logger.getLogger(CouchbaseClientFactory.class);

    public static final String CACHE_HOSTS_PROPERTY = "hibernate.cache.couchbase.hosts";
    public static final String CACHE_BUCKET_PROPERTY = "hibernate.cache.couchbase.bucket";
    public static final String CACHE_PASSWORD_PROPERTY = "hibernate.cache.couchbase.password";

    public MemcachedClient create(Properties props) throws Exception {
        String hosts = props.getProperty(CACHE_HOSTS_PROPERTY, "localhost");
        String bucketName = props.getProperty(CACHE_BUCKET_PROPERTY, "cache");
        String password = props.getProperty(CACHE_PASSWORD_PROPERTY, "");

        List<URI> uris = new ArrayList<URI>();
        for (String host : Arrays.asList(hosts.split("[, ]"))) {
            String hostWithPort = host.contains(":") ? host : host + ":8091";
            uris.add(URI.create("http://" + hostWithPort + "/pools"));
        }

        log.debugf("Starting with hosts: '%s' and bucket: %s", hosts, bucketName);
        return new CouchbaseClient(uris, bucketName, password);
    }
}
