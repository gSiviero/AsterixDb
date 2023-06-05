/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.dataflow.std.join;

import java.util.Comparator;

public class PartitionComparatorBuilder {
    Comparator<Partition> comparator;

    public void addInMemoryTupleComparator(boolean reversed) {
        Comparator<Partition> newComparator = Comparator.comparing(Partition::getTuplesInMemory);
        addComparator(reversed ? newComparator.reversed() : newComparator);
    }

    public void addBufferSizeComparator(boolean reversed) {
        Comparator<Partition> newComparator = Comparator.comparing(Partition::getMemoryUsed);
        addComparator(reversed ? newComparator.reversed() : newComparator);
    }

    public void addStatusComparator(boolean reversed) {
        Comparator<Partition> newComparator = Comparator.comparing(Partition::getSpilledStatus);
        addComparator(reversed ? newComparator.reversed() : newComparator);
    }

    private void addComparator(Comparator<Partition> newComparator) {
        if (comparator == null) {
            comparator = newComparator;
        } else {
            comparator.thenComparing(newComparator);
        }
    }

    public Comparator<Partition> build() {
        return comparator;
    }
}
