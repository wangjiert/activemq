/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.kahadb.disk.journal;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.activemq.store.kahadb.disk.journal.Journal.JournalDiskSyncStrategy;
import org.apache.activemq.store.kahadb.disk.util.DataByteArrayOutputStream;
import org.apache.activemq.store.kahadb.disk.util.LinkedNodeList;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.store.kahadb.disk.journal.Journal.EMPTY_BATCH_CONTROL_RECORD;
import static org.apache.activemq.store.kahadb.disk.journal.Journal.RECORD_HEAD_SPACE;

/**
 * An optimized writer to do batch appends to a data file. This object is thread
 * safe and gains throughput as you increase the number of concurrent writes it
 * does.
 */
class DataFileAppender implements FileAppender {

    private static final Logger logger = LoggerFactory.getLogger(DataFileAppender.class);

    protected final Journal journal;
    //只是记录了异步的写消息
    //和journal引用的是同一个集合
    protected final Map<Journal.WriteKey, Journal.WriteCommand> inflightWrites;
    protected final Object enqueueMutex = new Object();
    protected WriteBatch nextWriteBatch;

    protected boolean shutdown;
    protected IOException firstAsyncException;
    protected final CountDownLatch shutdownDone = new CountDownLatch(1);
    protected int maxWriteBatchSize;
    //异步同步就是每次写完一批就同步一次吗
    protected final boolean syncOnComplete;
    //这个应该就是启动个定时任务周期性执行吧
    protected final boolean periodicSync;

    protected boolean running;
    private Thread thread;

    public class WriteBatch {

        public final DataFile dataFile;

        public final LinkedNodeList<Journal.WriteCommand> writes = new LinkedNodeList<Journal.WriteCommand>();
        public final CountDownLatch latch = new CountDownLatch(1);
        protected final int offset;
        public int size = Journal.BATCH_CONTROL_RECORD_SIZE;
        public AtomicReference<IOException> exception = new AtomicReference<IOException>();

        public WriteBatch(DataFile dataFile,int offset) {
            this.dataFile = dataFile;
            this.offset = offset;
            this.dataFile.incrementLength(Journal.BATCH_CONTROL_RECORD_SIZE);
            this.size=Journal.BATCH_CONTROL_RECORD_SIZE;
            journal.addToTotalLength(Journal.BATCH_CONTROL_RECORD_SIZE);
        }

        public WriteBatch(DataFile dataFile, int offset, Journal.WriteCommand write) throws IOException {
            this(dataFile, offset);
            append(write);
        }

        public boolean canAppend(Journal.WriteCommand write) {
            int newSize = size + write.location.getSize();
            if (newSize >= maxWriteBatchSize || offset+newSize > journal.getMaxFileLength() ) {
                return false;
            }
            return true;
        }

        public void append(Journal.WriteCommand write) throws IOException {
            this.writes.addLast(write);
            write.location.setDataFileId(dataFile.getDataFileId());
            write.location.setOffset(offset+size);
            int s = write.location.getSize();
            size += s;
            dataFile.incrementLength(s);
            journal.addToTotalLength(s);
        }
    }

    /**
     * Construct a Store writer
     */
    public DataFileAppender(Journal dataManager) {
        this.journal = dataManager;
        //这个应该都是只有一个对象的吧
        //为什么要引用同一个对象呢 为什么不通过journal间接的来访问这个map呢
        this.inflightWrites = this.journal.getInflightWrites();
        this.maxWriteBatchSize = this.journal.getWriteBatchSize();
        this.syncOnComplete = this.journal.isEnableAsyncDiskSync();
        this.periodicSync = JournalDiskSyncStrategy.PERIODIC.equals(
                this.journal.getJournalDiskSyncStrategy());
    }

    @Override
    public Location storeItem(ByteSequence data, byte type, boolean sync) throws IOException {

        // Write the packet our internal buffer.
        //前5字节记录的是什么呢
        int size = data.getLength() + RECORD_HEAD_SPACE;

        final Location location = new Location();
        location.setSize(size);
        location.setType(type);

        Journal.WriteCommand write = new Journal.WriteCommand(location, data, sync);

        WriteBatch batch = enqueue(write);
        //难道说是通过这个批处理可以知道数据存在哪 但是批处理是异步的 而且也没有存任何存储位置相关的信息啊
        //索引里面是存了这个location的 那么这个有什么用呢 就是记录一下消息大小吗
        //索引中不是以消息id做key location做值吗 这个意思不就是可以通过消息id找到消息具体存在哪吗
        //这样分析 很矛盾啊
        //之前看的都是什么  enqueue的时候明明就记录了消息的具体位置 在调用返回之前 就已经决定了消息具体写到哪里
        location.setBatch(batch);
        if (sync) {
            try {
                batch.latch.await();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
            IOException exception = batch.exception.get();
            if (exception != null) {
                throw exception;
            }
        }

        return location;
    }

    @Override
    public Location storeItem(ByteSequence data, byte type, Runnable onComplete) throws IOException {
        // Write the packet our internal buffer.
        int size = data.getLength() + RECORD_HEAD_SPACE;

        final Location location = new Location();
        location.setSize(size);
        location.setType(type);

        Journal.WriteCommand write = new Journal.WriteCommand(location, data, onComplete);
        location.setBatch(enqueue(write));

        return location;
    }

    private WriteBatch enqueue(Journal.WriteCommand write) throws IOException {
        synchronized (enqueueMutex) {
            if (shutdown) {
                throw new IOException("Async Writer Thread Shutdown");
            }

            if (!running) {
                running = true;
                thread = new Thread() {
                    @Override
                    public void run() {
                        processQueue();
                    }
                };
                thread.setPriority(Thread.MAX_PRIORITY);
                thread.setDaemon(true);
                thread.setName("ActiveMQ Data File Writer");
                thread.start();
            }

            while ( true ) {
                if (nextWriteBatch == null) {
                    //会不会有一个超大消息直接超过文件最大长度
                    DataFile file = journal.getCurrentDataFile(write.location.getSize());
                    nextWriteBatch = newWriteBatch(write, file);
                    enqueueMutex.notifyAll();
                    break;
                } else {
                    // Append to current batch if possible..
                    if (nextWriteBatch.canAppend(write)) {
                        nextWriteBatch.append(write);
                        break;
                    } else {
                        // Otherwise wait for the queuedCommand to be null
                        try {
                            while (nextWriteBatch != null) {
                                final long start = System.currentTimeMillis();
                                enqueueMutex.wait();
                                if (maxStat > 0) {
                                    logger.info("Waiting for write to finish with full batch... millis: " +
                                                (System.currentTimeMillis() - start));
                               }
                            }
                        } catch (InterruptedException e) {
                            throw new InterruptedIOException();
                        }
                        if (shutdown) {
                            throw new IOException("Async Writer Thread Shutdown");
                        }
                    }
                }
            }
            if (!write.sync) {
                inflightWrites.put(new Journal.WriteKey(write.location), write);
            }
            return nextWriteBatch;
        }
    }

    protected WriteBatch newWriteBatch(Journal.WriteCommand write, DataFile file) throws IOException {
        return new WriteBatch(file, file.getLength(), write);
    }

    @Override
    public void close() throws IOException {
        synchronized (enqueueMutex) {
            if (!shutdown) {
                shutdown = true;
                if (running) {
                    enqueueMutex.notifyAll();
                } else {
                    shutdownDone.countDown();
                }
            }
        }

        try {
            shutdownDone.await();
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }

    }

    int statIdx = 0;
    int[] stats = new int[maxStat];
    /**
     * The async processing loop that writes to the data files and does the
     * force calls. Since the file sync() call is the slowest of all the
     * operations, this algorithm tries to 'batch' or group together several
     * file sync() requests into a single file sync() call. The batching is
     * accomplished attaching the same CountDownLatch instance to every force
     * request in a group.
     */
    protected void  processQueue() {
        DataFile dataFile = null;
        RecoverableRandomAccessFile file = null;
        WriteBatch wb = null;
        try (DataByteArrayOutputStream buff = new DataByteArrayOutputStream(maxWriteBatchSize);) {

            while (true) {

                // Block till we get a command.
                synchronized (enqueueMutex) {
                    while (true) {
                        if (nextWriteBatch != null) {
                            wb = nextWriteBatch;
                            nextWriteBatch = null;
                            break;
                        }
                        if (shutdown) {
                            return;
                        }
                        enqueueMutex.wait();
                    }
                    enqueueMutex.notifyAll();
                }

                if (dataFile != wb.dataFile) {
                    if (file != null) {
                        if (periodicSync) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Syncing file {} on rotate", dataFile.getFile().getName());
                            }
                            file.sync();
                        }
                        dataFile.closeRandomAccessFile(file);
                    }
                    dataFile = wb.dataFile;
                    file = dataFile.appendRandomAccessFile();
                }

                Journal.WriteCommand write = wb.writes.getHead();

                // Write an empty batch control record.
                buff.reset();
                //写这个干什么
                //先写头部信息 一批数据写完了再回头改里面的数据大小和校验值
                buff.write(EMPTY_BATCH_CONTROL_RECORD);

                boolean forceToDisk = false;
                while (write != null) {
                    forceToDisk |= write.sync | (syncOnComplete && write.onComplete != null);
                    //额外的五个字节 用来记录数据的大小和消息类型 数据大小已经包含了这五个字节
                    buff.writeInt(write.location.getSize());
                    buff.writeByte(write.location.getType());
                    buff.write(write.data.getData(), write.data.getOffset(), write.data.getLength());
                    write = write.getNext();
                }

                // append 'unset', zero length next batch so read can always find eof
                //看来每一个批处理完都有一个EOF啊
                buff.write(Journal.EOF_RECORD);

                //最底层的字节数组还是只有一个
                ByteSequence sequence = buff.toByteSequence();

                // Now we can fill in the batch control record properly.
                //重置偏移量而已
                buff.reset();
                buff.skip(RECORD_HEAD_SPACE + Journal.BATCH_CONTROL_RECORD_MAGIC.length);
                buff.writeInt(sequence.getLength() - Journal.BATCH_CONTROL_RECORD_SIZE - Journal.EOF_RECORD.length);
                if( journal.isChecksum() ) {
                    Checksum checksum = new Adler32();
                    checksum.update(sequence.getData(), sequence.getOffset()+Journal.BATCH_CONTROL_RECORD_SIZE, sequence.getLength()-Journal.BATCH_CONTROL_RECORD_SIZE-Journal.EOF_RECORD.length);
                    buff.writeLong(checksum.getValue());
                }

                // Now do the 1 big write.
                file.seek(wb.offset);
                if (maxStat > 0) {
                    if (statIdx < maxStat) {
                        stats[statIdx++] = sequence.getLength();
                    } else {
                        long all = 0;
                        for (;statIdx > 0;) {
                            all+= stats[--statIdx];
                        }
                        logger.info("Ave writeSize: " + all/maxStat);
                    }
                }
                //写到文件了
                file.write(sequence.getData(), sequence.getOffset(), sequence.getLength());

                //暂时没有用
                ReplicationTarget replicationTarget = journal.getReplicationTarget();
                if( replicationTarget!=null ) {
                    replicationTarget.replicate(wb.writes.getHead().location, sequence, forceToDisk);
                }

                if (forceToDisk) {
                    file.sync();
                }

                Journal.WriteCommand lastWrite = wb.writes.getTail();
                journal.setLastAppendLocation(lastWrite.location);

                signalDone(wb);
            }
        } catch (Throwable error) {
            logger.warn("Journal failed while writing at: " + wb.dataFile.getDataFileId() + ":" + wb.offset, error);
            synchronized (enqueueMutex) {
                shutdown = true;
                running = false;
                signalError(wb, error);
                if (nextWriteBatch != null) {
                    signalError(nextWriteBatch, error);
                    nextWriteBatch = null;
                    enqueueMutex.notifyAll();
                }
            }
        } finally {
            try {
                if (file != null) {
                    if (periodicSync) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Syning file {} on close", dataFile.getFile().getName());
                        }
                        file.sync();
                    }
                    dataFile.closeRandomAccessFile(file);
                }
            } catch (Throwable ignore) {
            }
            shutdownDone.countDown();
            running = false;
        }
    }

    protected void signalDone(WriteBatch wb) {
        // Now that the data is on disk, remove the writes from the in
        // flight
        // cache.
        Journal.WriteCommand write = wb.writes.getHead();
        while (write != null) {
            if (!write.sync) {
                inflightWrites.remove(new Journal.WriteKey(write.location));
            }
            if (write.onComplete != null && wb.exception.get() == null) {
                try {
                    write.onComplete.run();
                } catch (Throwable e) {
                    logger.info("Add exception was raised while executing the run command for onComplete", e);
                }
            }
            write = write.getNext();
        }

        // Signal any waiting threads that the write is on disk.
        wb.latch.countDown();
    }

    protected void signalError(WriteBatch wb, Throwable t) {
        if (wb != null) {
            if (t instanceof IOException) {
                wb.exception.set((IOException) t);
                // revert sync batch increment such that next write is contiguous
                if (syncBatch(wb.writes)) {
                    wb.dataFile.decrementLength(wb.size);
                }
            } else {
                wb.exception.set(IOExceptionSupport.create(t));
            }
            signalDone(wb);
        }
    }

    // async writes will already be in the index so reuse is not an option
    private boolean syncBatch(LinkedNodeList<Journal.WriteCommand> writes) {
        Journal.WriteCommand write = writes.getHead();
        while (write != null && write.sync) {
            write = write.getNext();
        }
        return write == null;
    }
}
