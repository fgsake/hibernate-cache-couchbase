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

/**
 * Timestamp source. Relies on {@link System#currentTimeMillis()} and
 * {@link System#nanoTime()}.
 * <p>
 * We can't use only {@code System.currentTimeMillis()} as it's non-monotonic.
 * We can't use only {@code System.nanoTime()} as it's only meaningful as a
 * duration. If {@code System.currentTimeMillis()} is accurate when we call it
 * (and that's a pretty big if) then this scheme will work fairly well AS LONG
 * AS EVERYONE'S CLOCKS ARE IN SYNC.
 */
public final class Timestamper {

    private static long startTime = System.currentTimeMillis() * 1000;
    private static long startNanos = System.nanoTime();

    private Timestamper() {}

    public static long nextTimestamp() {
        return startTime + (System.nanoTime() - startNanos) / 1000;
    }
}
