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
package org.apache.asterix.column.values.reader.value;

import java.io.IOException;

import org.apache.asterix.column.bytes.decoder.ParquetPlainFixedLengthValuesReader;
import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.om.types.ATypeTag;

public final class DoubleValueReader extends AbstractValueReader {
    private final ParquetPlainFixedLengthValuesReader doubleReader;
    private double nextValue;

    public DoubleValueReader() {
        doubleReader = new ParquetPlainFixedLengthValuesReader(Double.BYTES);
    }

    @Override
    public void init(AbstractBytesInputStream in, int tupleCount) throws IOException {
        doubleReader.initFromPage(in);
    }

    @Override
    public void nextValue() {
        nextValue = doubleReader.readDouble();
    }

    @Override
    public double getDouble() {
        return nextValue;
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.DOUBLE;
    }

    @Override
    public int compareTo(AbstractValueReader o) {
        return Double.compare(nextValue, o.getDouble());
    }
}
