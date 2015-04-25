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

import java.io.*;
import java.util.Comparator;

public final class CacheLock implements CacheItem, Externalizable {
    private int count;
    private boolean concurrent;
    private long unlockTimestamp;
    private long timeout;
    private Object version;

    public CacheLock() {}

    public CacheLock(Object version, long timeout) {
        count = 1;
        concurrent = false;
        unlockTimestamp = Long.MAX_VALUE;
        this.timeout = timeout;
        this.version = version;
    }

    public boolean writable(long txTimestamp, Object version, Comparator versionComparator) {
        if (txTimestamp > timeout) {
            return true;
        } else if (count > 0) {
            return false;
        } else if (txTimestamp < unlockTimestamp) {
            return false;
        } else if (this.version != null) {
            return versionComparator.compare(this.version, version) < 0;
        } else {
            return true;
        }
    }

    public void lock(long timeout) {
        if (++count > 1) {
            concurrent = true;
        }
        unlockTimestamp = Long.MAX_VALUE;
        this.timeout = timeout;
    }

    public boolean unlock(long txTimestamp) {
        if (--count == 0 && !concurrent) {
            unlockTimestamp = txTimestamp;
            return true;
        }
        return false;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(1);
        out.writeInt(count);
        out.writeBoolean(concurrent);
        out.writeLong(unlockTimestamp);
        out.writeObject(version);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte version = in.readByte();
        if (version != 1) {
            throw new InvalidObjectException("Unsupported format version " + version);
        }
        count = in.readInt();
        concurrent = in.readBoolean();
        unlockTimestamp = in.readLong();
        this.version = in.readObject();
    }

    @Override
    public String toString() {
        return new StringBuilder("CacheLock{count=")
                .append(count)
                .append(", concurrent=")
                .append(concurrent)
                .append(", unlockTimestamp=")
                .append(unlockTimestamp)
                .append(", timeout=")
                .append(timeout)
                .append(", version=")
                .append(version)
                .append('}')
                .toString();
    }
}
