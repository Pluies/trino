/*
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
package io.trino.filesystem.alluxio;

import alluxio.client.file.FileInStream;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Mostly copy pasted from AlluxioHdfsInputStream
 * https://github.com/Alluxio/alluxio/blob/903269f077f30d3c274adfbc53ef248be8ff7356/core/client/hdfs/src/main/java/alluxio/hadoop/AlluxioHdfsInputStream.java#L26
 */
class AlluxioTrinoInputStream
        extends FileInStream
{
    private final TrinoInputStream mInput;

    public AlluxioTrinoInputStream(TrinoInputStream input)
    {
        this.mInput = requireNonNull(input);
    }

    @Override
    public int read(byte[] bytes)
            throws IOException
    {
        return this.mInput.read(bytes);
    }

    @Override
    public int read(byte[] bytes, int offset, int length)
            throws IOException
    {
        return this.mInput.read(bytes, offset, length);
    }

    @Override
    public int read()
            throws IOException
    {
        return this.mInput.read();
    }

    @Override
    public int read(ByteBuffer buf)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long skip(long length)
            throws IOException
    {
        return this.mInput.skip(length);
    }

    @Override
    public int available()
            throws IOException
    {
        return this.mInput.available();
    }

    @Override
    public void close()
            throws IOException
    {
        this.mInput.close();
    }

    @Override
    public synchronized void mark(int limit)
    {
        this.mInput.mark(limit);
    }

    @Override
    public synchronized void reset()
            throws IOException
    {
        this.mInput.reset();
    }

    @Override
    public boolean markSupported()
    {
        return this.mInput.markSupported();
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        this.mInput.seek(position);
    }

    @Override
    public long getPos()
            throws IOException
    {
        return this.mInput.getPosition();
    }

    @Override
    public long remaining()
    {
        throw new UnsupportedOperationException("Remaining is not supported");
    }

    @Override
    public int positionedRead(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unbuffer()
    {
        throw new UnsupportedOperationException();
    }
}
