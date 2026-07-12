/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.network.netty;

import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.TransferableChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Drains a {@link Send} into a Netty {@link ByteBuf} without copying heap payloads.
 *
 * <p>{@code SendBuilder} produces sends whose frames already contain the 4-byte length prefix,
 * so the resulting buffer is written to the channel as-is. Heap {@code ByteBuffer} components
 * (including the zero-copy record payloads retained by reference in produce requests) are
 * wrapped, not copied; the caller must keep them unmodified until the channel write completes.
 * File-backed components (broker-side fetch responses) are read into pooled buffers — the
 * {@code FileRegion} translation is a later, server-transport deliverable.
 */
final class ByteBufSinkChannel implements TransferableChannel {

    private final ByteBufAllocator allocator;
    private final CompositeByteBuf composite;
    private boolean open = true;

    ByteBufSinkChannel(ByteBufAllocator allocator) {
        this.allocator = allocator;
        this.composite = allocator.compositeBuffer(64);
    }

    /** Fully drain the given send into a single (composite) buffer. */
    static ByteBuf drain(Send send, ByteBufAllocator allocator) throws IOException {
        ByteBufSinkChannel sink = new ByteBufSinkChannel(allocator);
        while (!send.completed()) {
            long written = send.writeTo(sink);
            if (written < 0)
                throw new IOException("Send reported negative bytes written");
        }
        return sink.finish();
    }

    ByteBuf finish() {
        open = false;
        return composite;
    }

    @Override
    public int write(ByteBuffer src) {
        int remaining = src.remaining();
        if (remaining > 0) {
            // Wrap a duplicate so the source buffer's position is under our control; the
            // underlying bytes are shared, not copied.
            composite.addComponent(true, Unpooled.wrappedBuffer(src.duplicate()));
            src.position(src.limit());
        }
        return remaining;
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) {
        long total = 0;
        for (int i = offset; i < offset + length; i++)
            total += write(srcs[i]);
        return total;
    }

    @Override
    public long write(ByteBuffer[] srcs) {
        return write(srcs, 0, srcs.length);
    }

    @Override
    public boolean hasPendingWrites() {
        return false;
    }

    @Override
    public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
        int chunk = (int) Math.min(count, 1 << 20);
        ByteBuf buf = allocator.directBuffer(chunk);
        try {
            int read = buf.writeBytes(fileChannel, position, chunk);
            if (read <= 0) {
                buf.release();
                return read;
            }
            composite.addComponent(true, buf);
            return read;
        } catch (IOException e) {
            buf.release();
            throw e;
        }
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() {
        open = false;
    }
}
