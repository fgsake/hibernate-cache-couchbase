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

import java.io.*;
import java.util.Comparator;

public final class CacheData implements Externalizable, CacheItem {
    private long timestamp;
    private Object version;
    private Object value;

    public CacheData() {}

    public CacheData(long timestamp, Object version, Object value) {
        this.timestamp = timestamp;
        this.version = version;
        this.value = value;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(1); // format version
        out.writeLong(timestamp);
        out.writeObject(version);
        out.writeObject(value);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte version = in.readByte();
        if (version != 1) {
            throw new InvalidObjectException("Unrecognized version: " + version);
        }

        timestamp = in.readLong();
        this.version = in.readObject();
        value = in.readObject();
    }

    public boolean writable(long txTimestamp, Object version, Comparator versionComparator) {
        return this.version != null && versionComparator.compare(this.version, version) < 0;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return new StringBuilder("CacheData{timestamp=")
                .append(timestamp)
                .append(", version=")
                .append(version)
                .append(", value=")
                .append(value)
                .append('}')
                .toString();
    }
}
