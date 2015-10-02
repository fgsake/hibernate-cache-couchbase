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
    private int schemaVersion;

    public CacheData() {
    }

    public CacheData(long timestamp, Object version, Object value, int schemaVersion) {
        this.timestamp = timestamp;
        this.version = version;
        this.value = value;
        this.schemaVersion = schemaVersion;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(2); // format version
        out.writeLong(timestamp);
        out.writeObject(version);
        out.writeObject(value);
        out.writeInt(schemaVersion);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte version = in.readByte();
        switch (version) {
            case 1:
                timestamp = in.readLong();
                this.version = in.readObject();
                value = in.readObject();
                schemaVersion = Integer.MIN_VALUE;
                break;
            case 2:
                timestamp = in.readLong();
                this.version = in.readObject();
                value = in.readObject();
                schemaVersion = in.readInt();
                break;
            default:
                timestamp = in.readLong();
                this.version = in.readObject();
                value = in.readObject();
                schemaVersion = in.readInt();
                break;
        }
    }

    public boolean writable(long txTimestamp, Object version, Comparator versionComparator, int schemaVersion) {
        return this.version != null && versionComparator.compare(this.version, version) < 0 || schemaVersion > this.schemaVersion;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Object getValue() {
        return value;
    }

    public int getSchemaVersion() {
        return schemaVersion;
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
