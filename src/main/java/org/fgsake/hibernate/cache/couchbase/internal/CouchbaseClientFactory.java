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

    /**
     * A comma-separated string of hosts to bootstrap the client. For each host,
     * the port defaults to 8091 if not specified. Default is localhost.
     * <p>
     * NOTE: Only <code>host:port</code> is required. Using a URL won't work.
     */
    public static final String CACHE_HOSTS_PROPERTY = "hibernate.cache.couchbase.hosts";
    /**
     * The Couchbase bucket to use. Default is cache.
     */
    public static final String CACHE_BUCKET_PROPERTY = "hibernate.cache.couchbase.bucket";
    /**
     * The bucket password. Default is the empty string.
     */
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
