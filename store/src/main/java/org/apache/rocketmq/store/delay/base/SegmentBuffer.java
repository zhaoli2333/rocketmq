/*
 * Copyright 2018 Qunar, Inc.
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

package org.apache.rocketmq.store.delay.base;


import java.nio.ByteBuffer;

public class SegmentBuffer {
    private final long startOffset;
    private final int size;

    private final ByteBuffer buffer;

    public SegmentBuffer(long startOffset, ByteBuffer buffer, int size) {
        this.startOffset = startOffset;
        this.size = size;
        this.buffer = buffer;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int getSize() {
        return size;
    }

}
