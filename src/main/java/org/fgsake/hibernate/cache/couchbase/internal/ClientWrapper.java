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
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import org.hibernate.cache.CacheException;
import org.jboss.logging.Logger;

import java.util.concurrent.TimeUnit;

public class ClientWrapper {
    private static final Logger log = Logger.getLogger(ClientWrapper.class);

    private final MemcachedClient client;

    public ClientWrapper(MemcachedClient client) {
        this.client = client;
    }

    public OperationFuture<Boolean> add(String key, int exp, Object o) {
        try {
            OperationFuture<Boolean> future = client.add(key, exp, o);
            if (log.isTraceEnabled()) {
                future.addListener(new LogListener("add"));
            }
            return future;
        } catch (IllegalStateException e) {
            throw new CacheException("Client command queue is full", e);
        } catch (RuntimeException e) {
            Throwable t = e.getCause() != null ? e.getCause() : e;
            throw new CacheException("Exception talking to Couchbase", t);
        }
    }

    public OperationFuture<CASResponse> asyncCAS(String key, long casId, int exp, Object value) {
        try {
            OperationFuture<CASResponse> future = client.asyncCAS(key, casId, exp, value);
            if (log.isTraceEnabled()) {
                future.addListener(new LogListener("cas"));
            }
            return future;
        } catch (IllegalStateException e) {
            throw new CacheException("Client command queue is full", e);
        } catch (RuntimeException e) {
            Throwable t = e.getCause() != null ? e.getCause() : e;
            throw new CacheException("Exception talking to Couchbase", t);
        }
    }

    public CASResponse cas(String key, long casId, int exp, Object value) {
        try {
            CASResponse rsp = client.cas(key, casId, exp, value);
            log.tracef("cas %s: %s", key, rsp);
            return rsp;
        } catch (OperationTimeoutException e) {
            throw new CacheException("Couchbase unavailable", e);
        } catch (IllegalStateException e) {
            throw new CacheException("Client command queue is full", e);
        } catch (RuntimeException e) {
            Throwable t = e.getCause() != null ? e.getCause() : e;
            throw new CacheException("Exception talking to Couchbase", t);
        }
    }

    public OperationFuture<Boolean> delete(String key) {
        try {
            OperationFuture<Boolean> future = client.delete(key);
            if (log.isTraceEnabled()) {
                future.addListener(new LogListener("delete"));
            }
            return future;
        } catch (IllegalStateException e) {
            throw new CacheException("Client command queue is full", e);
        } catch (RuntimeException e) {
            Throwable t = e.getCause() != null ? e.getCause() : e;
            throw new CacheException("Exception talking to Couchbase", t);
        }
    }

    public Object get(String key) {
        try {
            Object value = client.get(key);
            log.tracef("get %s: %s", key, value);
            return value;
        } catch (OperationTimeoutException e) {
            throw new CacheException("Couchbase unavailable", e);
        } catch (IllegalStateException e) {
            throw new CacheException("Client command queue is full", e);
        } catch (RuntimeException e) {
            Throwable t = e.getCause() != null ? e.getCause() : e;
            throw new CacheException("Exception talking to Couchbase", t);
        }
    }

    public CASValue<Object> gets(String key) {
        try {
            CASValue<Object> value = client.gets(key);
            log.tracef("gets %s: %s", key, value);
            return value;
        } catch (OperationTimeoutException e) {
            throw new CacheException("Couchbase unavailable", e);
        } catch (IllegalStateException e) {
            throw new CacheException("Client command queue is full", e);
        } catch (RuntimeException e) {
            Throwable t = e.getCause() != null ? e.getCause() : e;
            throw new CacheException("Exception talking to Couchbase", t);
        }
    }

    public OperationFuture<Boolean> set(String key, int exp, Object o) {
        try {
            OperationFuture<Boolean> future = client.set(key, exp, o);
            if (log.isTraceEnabled()) {
                future.addListener(new LogListener("set"));
            }
            return future;
        } catch (IllegalStateException e) {
            throw new CacheException("Client command queue is full", e);
        } catch (RuntimeException e) {
            Throwable t = e.getCause() != null ? e.getCause() : e;
            throw new CacheException("Exception talking to Couchbase", t);
        }
    }

    public void shutdown(long timeout, TimeUnit unit) {
        client.shutdown(timeout, unit);
    }

    private static class LogListener implements OperationCompletionListener {
        private final String op;

        public LogListener(String op) {
            this.op = op;
        }

        public void onComplete(OperationFuture<?> future) throws Exception {
            log.tracef("%s on %s: %s", op, future.getKey(), future.getStatus().getStatusCode());
        }
    }
}
