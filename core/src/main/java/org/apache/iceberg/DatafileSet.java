/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;

public class DatafileSet implements Set<DataFile> {

    private final Map<Integer, PartitionSpec> specs;
    private PatriciaTrie<DataFile> patriciaTrie;

    public DatafileSet(Map<Integer, PartitionSpec> specs) {
        this.patriciaTrie = new PatriciaTrie<>();
        this.specs = specs;
    }

    @Override
    public int size() {
        return patriciaTrie.size();
    }

    @Override
    public boolean isEmpty() {
        return patriciaTrie.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof DataFile) {
            DataFile dataFile = (DataFile) o;
            return patriciaTrie.containsKey(dataFile.path().toString());
        }

        return false;
    }

    @Override
    public Iterator<DataFile> iterator() {
        return patriciaTrie.values().iterator();
    }

    @Override
    public Object[] toArray() {
        return Iterators.toArray(iterator(), DataFile.class);
    }

    @Override
    public <T> T[] toArray(T[] destArray) {
        int size = patriciaTrie.size();
        if (destArray.length < size) {
            return (T[]) toArray();
        }

        Iterator<DataFile> iter = iterator();
        int ind = 0;
        while (iter.hasNext()) {
            destArray[ind] = (T) iter.next();
            ind += 1;
        }

        if (destArray.length > size) {
            destArray[size] = null;
        }

        return destArray;
    }

    @Override
    public boolean add(DataFile dataFile) {
        DataFile copy = DataFiles.builder(specs.get(dataFile.specId())).copy(dataFile).withPath(null).build();
        return patriciaTrie.put(dataFile.path().toString(), copy) == null;
    }

    @Override
    public boolean remove(Object o) {
        return patriciaTrie.remove(o) != null;
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends DataFile> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        patriciaTrie.clear();
    }
}
