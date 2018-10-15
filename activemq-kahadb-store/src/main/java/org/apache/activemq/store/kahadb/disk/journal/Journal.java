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

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.activemq.store.kahadb.disk.util.LinkedNode;
import org.apache.activemq.store.kahadb.disk.util.LinkedNodeList;
import org.apache.activemq.store.kahadb.disk.util.Sequence;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.DataByteArrayInputStream;
import org.apache.activemq.util.DataByteArrayOutputStream;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.apache.activemq.util.ThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages DataFiles
 */
public class Journal {
    public static final String CALLER_BUFFER_APPENDER = "org.apache.kahadb.journal.CALLER_BUFFER_APPENDER";
    public static final boolean callerBufferAppender = Boolean.parseBoolean(System.getProperty(CALLER_BUFFER_APPENDER, "false"));

    private static final int PREALLOC_CHUNK_SIZE = 1024*1024;

    // ITEM_HEAD_SPACE = length + type+ reserved space + SOR
    public static final int RECORD_HEAD_SPACE = 4 + 1;

    public static final byte USER_RECORD_TYPE = 1;
    public static final byte BATCH_CONTROL_RECORD_TYPE = 2;
    // Batch Control Item holds a 4 byte size of the batch and a 8 byte checksum of the batch.
    public static final byte[] BATCH_CONTROL_RECORD_MAGIC = bytes("WRITE BATCH");
    //4+1表示
    public static final int BATCH_CONTROL_RECORD_SIZE = RECORD_HEAD_SPACE + BATCH_CONTROL_RECORD_MAGIC.length + 4 + 8;
    //先写四个字节表示头的长度 1字节表示记录类型 然后一个魔数的字符串
    //头的长度是4个字节表示自身长度 一个字节表示类型 魔数的长度 四个字节表示数据长度 8字节表示校验数据
    public static final byte[] BATCH_CONTROL_RECORD_HEADER = createBatchControlRecordHeader();
    public static final byte[] EMPTY_BATCH_CONTROL_RECORD = createEmptyBatchControlRecordHeader();
    public static final int EOF_INT = ByteBuffer.wrap(new byte[]{'-', 'q', 'M', 'a'}).getInt();
    public static final byte EOF_EOT = '4';
    //就是四个字节加一个字节的数据
    public static final byte[] EOF_RECORD = createEofBatchAndLocationRecord();

    private ScheduledExecutorService scheduler;

    // tackle corruption when checksum is disabled or corrupt with zeros, minimize data loss
    public void corruptRecoveryLocation(Location recoveryPosition) throws IOException {
        DataFile dataFile = getDataFile(recoveryPosition);
        // with corruption on recovery we have no faith in the content - slip to the next batch record or eof
        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        try {
            RandomAccessFile randomAccessFile = reader.getRaf().getRaf();
            randomAccessFile.seek(recoveryPosition.getOffset() + 1);
            byte[] data = new byte[getWriteBatchSize()];
            ByteSequence bs = new ByteSequence(data, 0, randomAccessFile.read(data));
            int nextOffset = 0;
            if (findNextBatchRecord(bs, randomAccessFile) >= 0) {
                nextOffset = Math.toIntExact(randomAccessFile.getFilePointer() - bs.remaining());
            } else {
                nextOffset = Math.toIntExact(randomAccessFile.length());
            }
            Sequence sequence = new Sequence(recoveryPosition.getOffset(), nextOffset - 1);
            LOG.warn("Corrupt journal records found in '{}' between offsets: {}", dataFile.getFile(), sequence);

            // skip corruption on getNextLocation
            recoveryPosition.setOffset(nextOffset);
            recoveryPosition.setSize(-1);

            dataFile.corruptedBlocks.add(sequence);
        } catch (IOException e) {
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }
    }

    public DataFileAccessorPool getAccessorPool() {
        return accessorPool;
    }

    public void allowIOResumption() {
        if (appender instanceof DataFileAppender) {
            DataFileAppender dataFileAppender = (DataFileAppender)appender;
            dataFileAppender.shutdown = false;
        }
    }

    public enum PreallocationStrategy {
        SPARSE_FILE,
        OS_KERNEL_COPY,
        ZEROS,
        CHUNKED_ZEROS;
    }

    public enum PreallocationScope {
        ENTIRE_JOURNAL,
        ENTIRE_JOURNAL_ASYNC,//这个是创建一个datafile的时候会直接创建下一个
        NONE;
    }

    public enum JournalDiskSyncStrategy {
        ALWAYS,
        PERIODIC,
        NEVER;
    }

    private static byte[] createBatchControlRecordHeader() {
        try (DataByteArrayOutputStream os = new DataByteArrayOutputStream();) {
            os.writeInt(BATCH_CONTROL_RECORD_SIZE);
            os.writeByte(BATCH_CONTROL_RECORD_TYPE);
            os.write(BATCH_CONTROL_RECORD_MAGIC);
            ByteSequence sequence = os.toByteSequence();
            sequence.compact();
            return sequence.getData();
        } catch (IOException e) {
            throw new RuntimeException("Could not create batch control record header.", e);
        }
    }

    private static byte[] createEmptyBatchControlRecordHeader() {
        try (DataByteArrayOutputStream os = new DataByteArrayOutputStream();) {
            os.writeInt(BATCH_CONTROL_RECORD_SIZE);
            os.writeByte(BATCH_CONTROL_RECORD_TYPE);
            os.write(BATCH_CONTROL_RECORD_MAGIC);
            os.writeInt(0);
            os.writeLong(0l);
            ByteSequence sequence = os.toByteSequence();
            sequence.compact();
            return sequence.getData();
        } catch (IOException e) {
            throw new RuntimeException("Could not create empty batch control record header.", e);
        }
    }

    private static byte[] createEofBatchAndLocationRecord() {
        try (DataByteArrayOutputStream os = new DataByteArrayOutputStream();) {
            os.writeInt(EOF_INT);
            os.writeByte(EOF_EOT);
            ByteSequence sequence = os.toByteSequence();
            sequence.compact();
            return sequence.getData();
        } catch (IOException e) {
            throw new RuntimeException("Could not create eof header.", e);
        }
    }

    public static final String DEFAULT_DIRECTORY = ".";
    public static final String DEFAULT_ARCHIVE_DIRECTORY = "data-archive";
    public static final String DEFAULT_FILE_PREFIX = "db-";
    public static final String DEFAULT_FILE_SUFFIX = ".log";
    //为什么文件要设置的这么小呢
    public static final int DEFAULT_MAX_FILE_LENGTH = 1024 * 1024 * 32;
    public static final int DEFAULT_CLEANUP_INTERVAL = 1000 * 30;
    public static final int DEFAULT_MAX_WRITE_BATCH_SIZE = 1024 * 1024 * 4;

    private static final Logger LOG = LoggerFactory.getLogger(Journal.class);

    protected final Map<WriteKey, WriteCommand> inflightWrites = new ConcurrentHashMap<WriteKey, WriteCommand>();

    //数据目录
    protected File directory = new File(DEFAULT_DIRECTORY);
    //归档目录
    protected File directoryArchive;
    //设置归档文件目录时会被置为true
    //表示归档文件能否被覆盖吧
    //既然有归档 那么文件应该不是直接删了吧 应该是可能归档可能删除吧
    private boolean directoryArchiveOverridden = false;

    protected String filePrefix = DEFAULT_FILE_PREFIX;
    protected String fileSuffix = DEFAULT_FILE_SUFFIX;
    protected boolean started;

    protected int maxFileLength = DEFAULT_MAX_FILE_LENGTH;
    //4m
    protected int writeBatchSize = DEFAULT_MAX_WRITE_BATCH_SIZE;

    //猜测这个是不停的写在文件的最后面
    //这个只是做了很多事 真正写数据到文件的还是下面的对象
    protected FileAppender appender;
    //这个应该是可以随便写在哪
    protected DataFileAccessorPool accessorPool;

    //这个里面放的最多  不管是不是合法的都在里面
    protected Map<Integer, DataFile> fileMap = new HashMap<Integer, DataFile>();
    protected Map<File, DataFile> fileByFileMap = new LinkedHashMap<File, DataFile>();
    //存放使用中的DataFile
    protected LinkedNodeList<DataFile> dataFiles = new LinkedNodeList<DataFile>();

    //最后一次的location
    //也记录了最后写入的消息的location
    protected final AtomicReference<Location> lastAppendLocation = new AtomicReference<Location>();
    //定期的清理工作 清理pool
    protected ScheduledFuture cleanupTask;
    //可能不是从0开始的  初始值是store传过来的 目前看肯定是0
    //记录了所有datafile的数据长度和
    protected AtomicLong totalLength = new AtomicLong();
    //是否归档吧
    protected boolean archiveDataLogs;
    private ReplicationTarget replicationTarget;
    protected boolean checksum;
    //启动的时候检查数据文件的非法数据
    protected boolean checkForCorruptionOnStartup;
    //这个值貌似和下面的同步策略不冲突
    //但是这个值是根据下面的策略算出来的 有点浪费
    protected boolean enableAsyncDiskSync = true;
    private int nextDataFileId = 1;
    private Object dataFileIdLock = new Object();
    private final AtomicReference<DataFile> currentDataFile = new AtomicReference<>(null);
    private volatile DataFile nextDataFile;

    protected PreallocationScope preallocationScope = PreallocationScope.ENTIRE_JOURNAL;
    protected PreallocationStrategy preallocationStrategy = PreallocationStrategy.SPARSE_FILE;
    //会被用吗
    private File osKernelCopyTemplateFile = null;
    private ByteBuffer preAllocateDirectBuffer = null;

    protected JournalDiskSyncStrategy journalDiskSyncStrategy = JournalDiskSyncStrategy.ALWAYS;

    public interface DataFileRemovedListener {
        void fileRemoved(DataFile datafile);
    }

    private DataFileRemovedListener dataFileRemovedListener;

    public synchronized void start() throws IOException {
        //防止多次启动
        if (started) {
            return;
        }

        //启动时间
        long start = System.currentTimeMillis();
        //需要看一下和appender有关系没
        //这个东西是不是只会读文件不写呢 但是明明是随机访问文件的一个对象
        accessorPool = new DataFileAccessorPool(this);
        //这么快就设为true了吗 明明还没启动啊
        started = true;

        //用来写数据进入磁盘文件的对象
        appender = callerBufferAppender ? new CallerBufferingDataFileAppender(this) : new DataFileAppender(this);

        //列出数据目录下所有的db-*.log文件
        File[] files = directory.listFiles(new FilenameFilter() {
            //看这个方法的参数 感觉会遍历子目录呀
            @Override
            public boolean accept(File dir, String n) {
                //只接受db-*.log文件
                return dir.equals(directory) && n.startsWith(filePrefix) && n.endsWith(fileSuffix);
            }
        });

        if (files != null) {
            for (File file : files) {
                try {
                    String n = file.getName();
                    String numStr = n.substring(filePrefix.length(), n.length()-fileSuffix.length());
                    int num = Integer.parseInt(numStr);
                    //数字很重要 需要看一下什么时候会删除里面没有数据的文件
                    DataFile dataFile = new DataFile(file, num);
                    //这个里面存的有点多啊 空文件也进去了 没看到删除的地方
                    fileMap.put(dataFile.getDataFileId(), dataFile);
                    totalLength.addAndGet(dataFile.getLength());
                } catch (NumberFormatException e) {
                    // Ignore file that do not match the pattern.
                }
            }

            // Sort the list so that we can link the DataFiles together in the
            // right order.
            LinkedList<DataFile> l = new LinkedList<>(fileMap.values());
            Collections.sort(l);
            for (DataFile df : l) {
                if (df.getLength() == 0) {
                    //空文件也不删 也不做处理
                    //但是一个文件为什么为空呢 难道是消费一个消息之后就被删除了吗
                    //如果是这样效率是不是有点不行啊 可以删索引 然后如果一个数据文件不被索引引用的话就把整个数据文件删掉多好
                    // possibly the result of a previous failed write
                    LOG.info("ignoring zero length, partially initialised journal data file: " + df);
                    continue;
                } else if (l.getLast().equals(df) && isUnusedPreallocated(df)) {
                    //需要看一下 预初始化的时候到底往文件写了些什么
                    //如果最后一个文件是预申请的 还没用也不处理
                    //预先申请时候是在文件头和文件尾部同时写了EOF 同时文件长度直接变成最大的文件长度
                    continue;
                }
                dataFiles.addLast(df);
                fileByFileMap.put(df.getFile(), df);

                if( isCheckForCorruptionOnStartup() ) {
                    //返回的location应该只有偏移量有用吧 记录的是文件校验合法的偏移量的后一位
                    lastAppendLocation.set(recoveryCheck(df));
                }
            }
        }

        //感觉没啥用
        if (preallocationScope != PreallocationScope.NONE) {
            switch (preallocationStrategy) {
                //这应该是文件的
                case SPARSE_FILE:
                    break;
                case OS_KERNEL_COPY: {
                    osKernelCopyTemplateFile = createJournalTemplateFile();
                }
                break;
                case CHUNKED_ZEROS: {
                    preAllocateDirectBuffer = allocateDirectBuffer(PREALLOC_CHUNK_SIZE);
                }
                break;
                case ZEROS: {
                    preAllocateDirectBuffer = allocateDirectBuffer(getMaxFileLength());
                }
                break;
            }
        }
        scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread schedulerThread = new Thread(r);
                schedulerThread.setName("ActiveMQ Journal Scheduled executor");
                schedulerThread.setDaemon(true);
                return schedulerThread;
            }
        });

        // init current write file
        if (dataFiles.isEmpty()) {
            nextDataFileId = 1;
            rotateWriteFile();
        } else {
            currentDataFile.set(dataFiles.getTail());
            nextDataFileId = currentDataFile.get().dataFileId + 1;
        }

        //对每个文件进行校验是可配置的 所以不校验或则逻辑没走到的的时候这个可能为空
        if( lastAppendLocation.get()==null ) {
            DataFile df = dataFiles.getTail();
            //为啥只校验最后一个文件呢
            lastAppendLocation.set(recoveryCheck(df));
        }

        // ensure we don't report unused space of last journal file in size metric
        int lastFileLength = dataFiles.getTail().getLength();
        //这是不是有问题啊 location记录的不一定是最后一个文件的偏移量啊
        if (totalLength.get() > lastFileLength && lastAppendLocation.get().getOffset() > 0) {
            //不是应该偏移量大于文件长度吗 所以这里是不是写反了
            totalLength.addAndGet(lastAppendLocation.get().getOffset() - lastFileLength);
        }

        cleanupTask = scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                //只是清楚pools里面打开的随机访问对象
                cleanup();
            }
        }, DEFAULT_CLEANUP_INTERVAL, DEFAULT_CLEANUP_INTERVAL, TimeUnit.MILLISECONDS);

        long end = System.currentTimeMillis();
        LOG.trace("Startup took: "+(end-start)+" ms");
    }

    private ByteBuffer allocateDirectBuffer(int size) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        buffer.put(EOF_RECORD);
        return buffer;
    }

    public void preallocateEntireJournalDataFile(RecoverableRandomAccessFile file) {

        if (PreallocationScope.NONE != preallocationScope) {

            try {
                if (PreallocationStrategy.OS_KERNEL_COPY == preallocationStrategy) {
                    doPreallocationKernelCopy(file);
                } else if (PreallocationStrategy.ZEROS == preallocationStrategy) {
                    doPreallocationZeros(file);
                } else if (PreallocationStrategy.CHUNKED_ZEROS == preallocationStrategy) {
                    doPreallocationChunkedZeros(file);
                } else {
                    doPreallocationSparseFile(file);
                }
            } catch (Throwable continueWithNoPrealloc) {
                // error on preallocation is non fatal, and we don't want to leak the journal handle
                LOG.error("cound not preallocate journal data file", continueWithNoPrealloc);
            }
        }
    }

    private void doPreallocationSparseFile(RecoverableRandomAccessFile file) {
        final ByteBuffer journalEof = ByteBuffer.wrap(EOF_RECORD);
        try {
            FileChannel channel = file.getChannel();
            channel.position(0);
            channel.write(journalEof);
            channel.position(maxFileLength - 5);
            journalEof.rewind();
            channel.write(journalEof);
            channel.force(false);
            channel.position(0);
        } catch (ClosedByInterruptException ignored) {
            LOG.trace("Could not preallocate journal file with sparse file", ignored);
        } catch (IOException e) {
            LOG.error("Could not preallocate journal file with sparse file", e);
        }
    }

    private void doPreallocationZeros(RecoverableRandomAccessFile file) {
        preAllocateDirectBuffer.rewind();
        try {
            FileChannel channel = file.getChannel();
            channel.write(preAllocateDirectBuffer);
            channel.force(false);
            channel.position(0);
        } catch (ClosedByInterruptException ignored) {
            LOG.trace("Could not preallocate journal file with zeros", ignored);
        } catch (IOException e) {
            LOG.error("Could not preallocate journal file with zeros", e);
        }
    }

    private void doPreallocationKernelCopy(RecoverableRandomAccessFile file) {
        try (RandomAccessFile templateRaf = new RandomAccessFile(osKernelCopyTemplateFile, "rw");){
            templateRaf.getChannel().transferTo(0, getMaxFileLength(), file.getChannel());
        } catch (ClosedByInterruptException ignored) {
            LOG.trace("Could not preallocate journal file with kernel copy", ignored);
        } catch (FileNotFoundException e) {
            LOG.error("Could not find the template file on disk at " + osKernelCopyTemplateFile.getAbsolutePath(), e);
        } catch (IOException e) {
            LOG.error("Could not transfer the template file to journal, transferFile=" + osKernelCopyTemplateFile.getAbsolutePath(), e);
        }
    }

    private File createJournalTemplateFile() {
        String fileName = "db-log.template";
        File rc = new File(directory, fileName);
        try (RandomAccessFile templateRaf = new RandomAccessFile(rc, "rw");) {
            templateRaf.getChannel().write(ByteBuffer.wrap(EOF_RECORD));
            templateRaf.setLength(maxFileLength);
            templateRaf.getChannel().force(true);
        } catch (FileNotFoundException e) {
            LOG.error("Could not find the template file on disk at " + osKernelCopyTemplateFile.getAbsolutePath(), e);
        } catch (IOException e) {
            LOG.error("Could not transfer the template file to journal, transferFile=" + osKernelCopyTemplateFile.getAbsolutePath(), e);
        }
        return rc;
    }

    private void doPreallocationChunkedZeros(RecoverableRandomAccessFile file) {
        preAllocateDirectBuffer.limit(preAllocateDirectBuffer.capacity());
        preAllocateDirectBuffer.rewind();
        try {
            FileChannel channel = file.getChannel();

            int remLen = maxFileLength;
            while (remLen > 0) {
                if (remLen < preAllocateDirectBuffer.remaining()) {
                    preAllocateDirectBuffer.limit(remLen);
                }
                int writeLen = channel.write(preAllocateDirectBuffer);
                remLen -= writeLen;
                preAllocateDirectBuffer.rewind();
            }

            channel.force(false);
            channel.position(0);
        } catch (ClosedByInterruptException ignored) {
            LOG.trace("Could not preallocate journal file with zeros", ignored);
        } catch (IOException e) {
            LOG.error("Could not preallocate journal file with zeros! Will continue without preallocation", e);
        }
    }

    private static byte[] bytes(String string) {
        try {
            return string.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isUnusedPreallocated(DataFile dataFile) throws IOException {
        //好像是一个创建一个新的数据文件时 会提前初始化下一个数据文件
        if (preallocationScope == PreallocationScope.ENTIRE_JOURNAL_ASYNC) {
            //从缓存里面拿 或者新建
            DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
            try {
                //5个字节加上魔数的长度
                byte[] firstFewBytes = new byte[BATCH_CONTROL_RECORD_HEADER.length];
                reader.readFully(0, firstFewBytes);
                ByteSequence bs = new ByteSequence(firstFewBytes);
                //检查空 只看前五个字节就够了
                return bs.startsWith(EOF_RECORD);
            } catch (Exception ignored) {
            } finally {
                accessorPool.closeDataFileAccessor(reader);
            }
        }
        return false;
    }

    protected Location recoveryCheck(DataFile dataFile) throws IOException {
        Location location = new Location();
        location.setDataFileId(dataFile.getDataFileId());
        location.setOffset(0);

        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        try {
            //随机读写的时候 如果在文件中间加数据 后面的数据是怎么移动的呢
            RandomAccessFile randomAccessFile = reader.getRaf().getRaf();
            randomAccessFile.seek(0);
            final long totalFileLength = randomAccessFile.length();
            //应该读的不是刚好一个批处理
            //一次读4m
            byte[] data = new byte[getWriteBatchSize()];
            ByteSequence bs = new ByteSequence(data, 0, randomAccessFile.read(data));

            //通过这个循环校验所有的批处理吗
            while (true) {
                //返回值大于等于0是对的否则发送了错误
                int size = checkBatchRecord(bs, randomAccessFile);
                //这是搞笑吗 文件里面读的数据的长度会超过文件自身的长度吗
                //到时可能 如果里面有EOF的话会导致条件判断的这种情况
                if (size >= 0 && location.getOffset() + BATCH_CONTROL_RECORD_SIZE + size <= totalFileLength) {
                    //找到一个EOF 就直接结束了 感觉这个EOF肯定不会是最后一个记录 不然不满足条件的后面
                    if (size == 0) {
                        // eof batch record
                        break;
                    }
                    //偏移量记录了下一个待检查待位置
                    location.setOffset(location.getOffset() + BATCH_CONTROL_RECORD_SIZE + size);
                } else {

                    //错误数据是有记录的
                    //如果最后一条记录是EOF会做什么处理呢 这里什么都没管
                    // Perhaps it's just some corruption... scan through the
                    // file to find the next valid batch record. We
                    // may have subsequent valid batch records.
                    //找到下一个批数据的头开始的位置
                    //找的过程中 找到头数据的时候并没有改变bs的偏移量啊
                    //很明显这个方法需要移动头数据到bs的开始位置
                    if (findNextBatchRecord(bs, randomAccessFile) >= 0) {
                        //文件偏移量减去批数据头信息开始之后的数据长度得出错误数据结束的地方
                        int nextOffset = Math.toIntExact(randomAccessFile.getFilePointer() - bs.remaining());
                        Sequence sequence = new Sequence(location.getOffset(), nextOffset - 1);
                        LOG.warn("Corrupt journal records found in '{}' between offsets: {}", dataFile.getFile(), sequence);
                        //这个for循环次数不少呀
                        dataFile.corruptedBlocks.add(sequence);
                        location.setOffset(nextOffset);
                    } else {
                        break;
                    }
                }
            }

        } catch (IOException e) {
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }

        int existingLen = dataFile.getLength();
        //在这里会直接截断数据文件的长度
        //location的偏移量表示 偏移量之前的数据都是校验过的 即使是错误的数据也会被记录
        //这里也会删掉EOF
        dataFile.setLength(location.getOffset());
        if (existingLen > dataFile.getLength()) {
            //总长度也会减少
            totalLength.addAndGet(dataFile.getLength() - existingLen);
        }

        //如果几个block连着也没关系 sequenset会自动连起来
        if (!dataFile.corruptedBlocks.isEmpty()) {
            // Is the end of the data file corrupted?
            if (dataFile.corruptedBlocks.getTail().getLast() + 1 == location.getOffset()) {
                //在这里改变了文件大小但是没改变总大小啊
                //写数据的时候难道不是根据这个判断从哪开始写吗
                //应该不是从底层文件的偏移量来的吧 之前所有的处理并没有改变文件的偏移量啊
                //最后一个文件这里的多余量会处理
                dataFile.setLength((int) dataFile.corruptedBlocks.removeLastSequence().getFirst());
            }
        }

        //这个东西也没管上一个处理中是否处理了
        return location;
    }

    private int findNextBatchRecord(ByteSequence bs, RandomAccessFile reader) throws IOException {
        final ByteSequence header = new ByteSequence(BATCH_CONTROL_RECORD_HEADER);
        int pos = 0;
        while (true) {
            pos = bs.indexOf(header, 0);
            if (pos >= 0) {//找到了头
                //这里处理了bs以批数据头开始
                bs.setOffset(bs.offset + pos);
                return pos;
            } else {
                // need to load the next data chunck in..
                if (bs.length != bs.data.length) {//文件读完了
                    // If we had a short read then we were at EOF
                    return -1;
                }
                //每次保留头数据的长度是为了防止头部信被截断
                //我觉得保留头部数据减一会不会更好
                bs.setOffset(bs.length - BATCH_CONTROL_RECORD_HEADER.length);
                bs.reset();
                bs.setLength(bs.length + reader.read(bs.data, bs.length, bs.data.length - BATCH_CONTROL_RECORD_HEADER.length));
            }
        }
    }

    //如果说文件里面没有记录数据只有一个初始化的终止记录返回0
    //如果说里面的数据的头信息不满足批处理的头返回-1
    //如果数据过短或过长返回-2
    //数据超过了文件长度返回-3
    //数据校验出错返回-4
    //正常情况下返回数据的长度
    private int checkBatchRecord(ByteSequence bs, RandomAccessFile reader) throws IOException {
        //只取五个字节是因为终止记录是五字节吗
        ensureAvailable(bs, reader, EOF_RECORD.length);
        if (bs.startsWith(EOF_RECORD)) {
            return 0; // eof
        }
        //确保有批处理头加上12字节那么长的数据
        ensureAvailable(bs, reader, BATCH_CONTROL_RECORD_SIZE);
        try (DataByteArrayInputStream controlIs = new DataByteArrayInputStream(bs)) {

            // Assert that it's a batch record.
            //对于一个批处理这是固定的值
            for (int i = 0; i < BATCH_CONTROL_RECORD_HEADER.length; i++) {
                if (controlIs.readByte() != BATCH_CONTROL_RECORD_HEADER[i]) {
                    return -1;
                }
            }

            //继续读取四字节的数据长度
            //需要看一下数据长度是否包含了头部的数据
            //
            int size = controlIs.readInt();
            //从这里看 只有最后的EOF被保存了 前面的EOF都被覆盖了
            //文件长度没这么大 因为长度是int类型的 所以这里判断的是批数据的最大的的可能的值
            //这里的判断也说明了数据的长度并不包括批处理头的长度
            if (size < 0 || size > Integer.MAX_VALUE - (BATCH_CONTROL_RECORD_SIZE + EOF_RECORD.length)) {
                return -2;
            }

            //数据校验
            //需要设置校验flag并且校验数据存在才会进行校验
            long expectedChecksum = controlIs.readLong();
            Checksum checksum = null;
            if (isChecksum() && expectedChecksum > 0) {
                checksum = new Adler32();
            }

            // revert to bs to consume data
            //跳过批处理头的长度加上12字节
            bs.setOffset(controlIs.position());
            //这是否说明数据的长度表示的就是数据本身的长度 不包含头尾的数据
            int toRead = size;
            //前面只是保证了头信息长度加上12字节的数据存在 数据是否全在并不保证所有要循环读
            //整个循环就是校验一下数据已经已经数据的长度是否正确
            while (toRead > 0) {
                if (bs.remaining() >= toRead) {//数据刚好够
                    if (checksum != null) {
                        checksum.update(bs.getData(), bs.getOffset(), toRead);
                    }
                    bs.setOffset(bs.offset + toRead);
                    toRead = 0;
                } else {
                    //每次读取数据的时候都会填满bs 这两个长度不相等说明已经读到了文件尾部
                    if (bs.length != bs.data.length) {
                        // buffer exhausted
                        return  -3;
                    }

                    toRead -= bs.remaining();
                    if (checksum != null) {
                        checksum.update(bs.getData(), bs.getOffset(), bs.remaining());
                    }
                    bs.setLength(reader.read(bs.data));
                    bs.setOffset(0);
                }
            }
            if (checksum != null && expectedChecksum != checksum.getValue()) {
                return -4;
            }

            return size;
        }
    }

    private void ensureAvailable(ByteSequence bs, RandomAccessFile reader, int required) throws IOException {
        if (bs.remaining() < required) {
            bs.reset();
            int read = reader.read(bs.data, bs.length, bs.data.length - bs.length);
            if (read < 0) {
                if (bs.remaining() == 0) {
                    throw new EOFException("request for " + required + " bytes reached EOF");
                }
            }
            //这是不是有问题呀 如果刚好遇到eof 那么返回值是-1 这会导致length减一吧
            bs.setLength(bs.length + read);
        }
    }

    void addToTotalLength(int size) {
        totalLength.addAndGet(size);
    }

    public long length() {
        return totalLength.get();
    }

    //创建新的datafile
    private void rotateWriteFile() throws IOException {
       synchronized (dataFileIdLock) {
            DataFile dataFile = nextDataFile;
            if (dataFile == null) {
                dataFile = newDataFile();
            }
            synchronized (currentDataFile) {
                fileMap.put(dataFile.getDataFileId(), dataFile);
                fileByFileMap.put(dataFile.getFile(), dataFile);
                dataFiles.addLast(dataFile);
                currentDataFile.set(dataFile);
            }
            nextDataFile = null;
        }
        if (PreallocationScope.ENTIRE_JOURNAL_ASYNC == preallocationScope) {
            //提前初始化下一个文件
            //返回的东西会用吗
            preAllocateNextDataFileFuture = scheduler.submit(preAllocateNextDataFileTask);
        }
    }

    private Runnable preAllocateNextDataFileTask = new Runnable() {
        @Override
        public void run() {
            if (nextDataFile == null) {
                synchronized (dataFileIdLock){
                    try {
                        nextDataFile = newDataFile();
                    } catch (IOException e) {
                        LOG.warn("Failed to proactively allocate data file", e);
                    }
                }
            }
        }
    };

    //看看谁用了
    private volatile Future preAllocateNextDataFileFuture;

    //都是直接用记录下一个文件id的变量
    private DataFile newDataFile() throws IOException {
        int nextNum = nextDataFileId++;
        File file = getFile(nextNum);
        DataFile nextWriteFile = new DataFile(file, nextNum);
        //创建一个随机访问对象
        //初始化策略在这里用到了
        //就是在文件开头和文件结尾写入EOF
        //是不是只要这里写了EOF其他地方都没有
        preallocateEntireJournalDataFile(nextWriteFile.appendRandomAccessFile());
        return nextWriteFile;
    }


    public DataFile reserveDataFile() {
        synchronized (dataFileIdLock) {
            int nextNum = nextDataFileId++;
            File file = getFile(nextNum);
            DataFile reservedDataFile = new DataFile(file, nextNum);
            synchronized (currentDataFile) {
                fileMap.put(reservedDataFile.getDataFileId(), reservedDataFile);
                fileByFileMap.put(file, reservedDataFile);
                if (dataFiles.isEmpty()) {
                    dataFiles.addLast(reservedDataFile);
                } else {
                    dataFiles.getTail().linkBefore(reservedDataFile);
                }
            }
            return reservedDataFile;
        }
    }

    public File getFile(int nextNum) {
        String fileName = filePrefix + nextNum + fileSuffix;
        File file = new File(directory, fileName);
        return file;
    }

    DataFile getDataFile(Location item) throws IOException {
        Integer key = Integer.valueOf(item.getDataFileId());
        DataFile dataFile = null;
        synchronized (currentDataFile) {
            dataFile = fileMap.get(key);
        }
        if (dataFile == null) {
            LOG.error("Looking for key " + key + " but not found in fileMap: " + fileMap);
            throw new IOException("Could not locate data file " + getFile(item.getDataFileId()));
        }
        return dataFile;
    }

    public void close() throws IOException {
        synchronized (this) {
            if (!started) {
                return;
            }
            cleanupTask.cancel(true);
            if (preAllocateNextDataFileFuture != null) {
                preAllocateNextDataFileFuture.cancel(true);
            }
            ThreadPoolUtils.shutdownGraceful(scheduler, 4000);
            accessorPool.close();
        }
        // the appender can be calling back to to the journal blocking a close AMQ-5620
        appender.close();
        synchronized (currentDataFile) {
            fileMap.clear();
            fileByFileMap.clear();
            dataFiles.clear();
            lastAppendLocation.set(null);
            started = false;
        }
    }

    public synchronized void cleanup() {
        if (accessorPool != null) {
            accessorPool.disposeUnused();
        }
    }

    public synchronized boolean delete() throws IOException {

        // Close all open file handles...
        appender.close();
        accessorPool.close();

        boolean result = true;
        for (Iterator<DataFile> i = fileMap.values().iterator(); i.hasNext();) {
            DataFile dataFile = i.next();
            result &= dataFile.delete();
        }

        if (preAllocateNextDataFileFuture != null) {
            preAllocateNextDataFileFuture.cancel(true);
        }
        synchronized (dataFileIdLock) {
            if (nextDataFile != null) {
                nextDataFile.delete();
                nextDataFile = null;
            }
        }

        totalLength.set(0);
        synchronized (currentDataFile) {
            fileMap.clear();
            fileByFileMap.clear();
            lastAppendLocation.set(null);
            dataFiles = new LinkedNodeList<DataFile>();
        }
        // reopen open file handles...
        //浪费呀 这个journal都没用了 可能是给其他地方调这个方法的吧
        accessorPool = new DataFileAccessorPool(this);
        appender = new DataFileAppender(this);
        return result;
    }

    public void removeDataFiles(Set<Integer> files) throws IOException {
        for (Integer key : files) {
            // Can't remove the data file (or subsequent files) that is currently being written to.
            if (key >= lastAppendLocation.get().getDataFileId()) {
                continue;
            }
            DataFile dataFile = null;
            synchronized (currentDataFile) {
                dataFile = fileMap.remove(key);
                if (dataFile != null) {
                    fileByFileMap.remove(dataFile.getFile());
                    dataFile.unlink();
                }
            }
            if (dataFile != null) {
                forceRemoveDataFile(dataFile);
            }
        }
    }

    private void forceRemoveDataFile(DataFile dataFile) throws IOException {
        accessorPool.disposeDataFileAccessors(dataFile);
        totalLength.addAndGet(-dataFile.getLength());
        if (archiveDataLogs) {
            File directoryArchive = getDirectoryArchive();
            if (directoryArchive.exists()) {
                LOG.debug("Archive directory exists: {}", directoryArchive);
            } else {
                if (directoryArchive.isAbsolute())
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Archive directory [{}] does not exist - creating it now",
                            directoryArchive.getAbsolutePath());
                }
                IOHelper.mkdirs(directoryArchive);
            }
            LOG.debug("Moving data file {} to {} ", dataFile, directoryArchive.getCanonicalPath());
            dataFile.move(directoryArchive);
            LOG.debug("Successfully moved data file");
        } else {
            LOG.debug("Deleting data file: {}", dataFile);
            if (dataFile.delete()) {
                LOG.debug("Discarded data file: {}", dataFile);
            } else {
                LOG.warn("Failed to discard data file : {}", dataFile.getFile());
            }
        }
        if (dataFileRemovedListener != null) {
            dataFileRemovedListener.fileRemoved(dataFile);
        }
    }

    /**
     * @return the maxFileLength
     */
    public int getMaxFileLength() {
        return maxFileLength;
    }

    /**
     * @param maxFileLength the maxFileLength to set
     */
    public void setMaxFileLength(int maxFileLength) {
        this.maxFileLength = maxFileLength;
    }

    @Override
    public String toString() {
        return directory.toString();
    }

    public Location getNextLocation(Location location) throws IOException, IllegalStateException {
        return getNextLocation(location, null);
    }

    public Location getNextLocation(Location location, Location limit) throws IOException, IllegalStateException {
        Location cur = null;
        while (true) {
            if (cur == null) {
                if (location == null) {
                    DataFile head = null;
                    synchronized (currentDataFile) {
                        head = dataFiles.getHead();
                    }
                    if (head == null) {
                        return null;
                    }
                    cur = new Location();
                    cur.setDataFileId(head.getDataFileId());
                    cur.setOffset(0);
                } else {
                    // Set to the next offset..
                    if (location.getSize() == -1) {
                        cur = new Location(location);
                    } else {
                        cur = new Location(location);
                        cur.setOffset(location.getOffset() + location.getSize());
                    }
                }
            } else {
                cur.setOffset(cur.getOffset() + cur.getSize());
            }

            DataFile dataFile = getDataFile(cur);

            // Did it go into the next file??
            if (dataFile.getLength() <= cur.getOffset()) {
                synchronized (currentDataFile) {
                    dataFile = dataFile.getNext();
                }
                if (dataFile == null) {
                    return null;
                } else {
                    cur.setDataFileId(dataFile.getDataFileId().intValue());
                    cur.setOffset(0);
                    if (limit != null && cur.compareTo(limit) >= 0) {
                        LOG.trace("reached limit: {} at: {}", limit, cur);
                        return null;
                    }
                }
            }

            // Load in location size and type.
            DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
            try {
                reader.readLocationDetails(cur);
            } catch (EOFException eof) {
                LOG.trace("EOF on next: " + location + ", cur: " + cur);
                throw eof;
            } finally {
                accessorPool.closeDataFileAccessor(reader);
            }

            Sequence corruptedRange = dataFile.corruptedBlocks.get(cur.getOffset());
            if (corruptedRange != null) {
                // skip corruption
                cur.setSize((int) corruptedRange.range());
            } else if (cur.getSize() == EOF_INT && cur.getType() == EOF_EOT ||
                    (cur.getType() == 0 && cur.getSize() == 0)) {
                // eof - jump to next datafile
                // EOF_INT and EOF_EOT replace 0,0 - we need to react to both for
                // replay of existing journals
                // possibly journal is larger than maxFileLength after config change
                cur.setSize(EOF_RECORD.length);
                cur.setOffset(Math.max(maxFileLength, dataFile.getLength()));
            } else if (cur.getType() == USER_RECORD_TYPE) {
                // Only return user records.
                return cur;
            }
        }
    }

    public ByteSequence read(Location location) throws IOException, IllegalStateException {
        DataFile dataFile = getDataFile(location);
        DataFileAccessor reader = accessorPool.openDataFileAccessor(dataFile);
        ByteSequence rc = null;
        try {
            rc = reader.readRecord(location);
        } finally {
            accessorPool.closeDataFileAccessor(reader);
        }
        return rc;
    }

    public Location write(ByteSequence data, boolean sync) throws IOException, IllegalStateException {
        //没记错的话 这个location没有记录消息具体存在哪
        //经验证 还是记了的
        Location loc = appender.storeItem(data, Location.USER_TYPE, sync);
        return loc;
    }

    public Location write(ByteSequence data, Runnable onComplete) throws IOException, IllegalStateException {
        Location loc = appender.storeItem(data, Location.USER_TYPE, onComplete);
        return loc;
    }

    public void update(Location location, ByteSequence data, boolean sync) throws IOException {
        DataFile dataFile = getDataFile(location);
        DataFileAccessor updater = accessorPool.openDataFileAccessor(dataFile);
        try {
            updater.updateRecord(location, data, sync);
        } finally {
            accessorPool.closeDataFileAccessor(updater);
        }
    }

    public PreallocationStrategy getPreallocationStrategy() {
        return preallocationStrategy;
    }

    public void setPreallocationStrategy(PreallocationStrategy preallocationStrategy) {
        this.preallocationStrategy = preallocationStrategy;
    }

    public PreallocationScope getPreallocationScope() {
        return preallocationScope;
    }

    public void setPreallocationScope(PreallocationScope preallocationScope) {
        this.preallocationScope = preallocationScope;
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public String getFilePrefix() {
        return filePrefix;
    }

    public void setFilePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
    }

    public Map<WriteKey, WriteCommand> getInflightWrites() {
        return inflightWrites;
    }

    public Location getLastAppendLocation() {
        return lastAppendLocation.get();
    }

    public void setLastAppendLocation(Location lastSyncedLocation) {
        this.lastAppendLocation.set(lastSyncedLocation);
    }

    public File getDirectoryArchive() {
        if (!directoryArchiveOverridden && (directoryArchive == null)) {
            // create the directoryArchive relative to the journal location
            directoryArchive = new File(directory.getAbsolutePath() +
                    File.separator + DEFAULT_ARCHIVE_DIRECTORY);
        }
        return directoryArchive;
    }

    public void setDirectoryArchive(File directoryArchive) {
        directoryArchiveOverridden = true;
        this.directoryArchive = directoryArchive;
    }

    public boolean isArchiveDataLogs() {
        return archiveDataLogs;
    }

    public void setArchiveDataLogs(boolean archiveDataLogs) {
        this.archiveDataLogs = archiveDataLogs;
    }

    public DataFile getDataFileById(int dataFileId) {
        synchronized (currentDataFile) {
            return fileMap.get(Integer.valueOf(dataFileId));
        }
    }

    public DataFile getCurrentDataFile(int capacity) throws IOException {
        //First just acquire the currentDataFile lock and return if no rotation needed
        synchronized (currentDataFile) {
            if (currentDataFile.get().getLength() + capacity < maxFileLength) {
                return currentDataFile.get();
            }
        }

        //AMQ-6545 - if rotation needed, acquire dataFileIdLock first to prevent deadlocks
        //then re-check if rotation is needed
        synchronized (dataFileIdLock) {
            synchronized (currentDataFile) {
                if (currentDataFile.get().getLength() + capacity >= maxFileLength) {
                    rotateWriteFile();
                }
                return currentDataFile.get();
            }
        }
    }

    public Integer getCurrentDataFileId() {
        synchronized (currentDataFile) {
            return currentDataFile.get().getDataFileId();
        }
    }

    /**
     * Get a set of files - only valid after start()
     *
     * @return files currently being used
     */
    public Set<File> getFiles() {
        synchronized (currentDataFile) {
            return fileByFileMap.keySet();
        }
    }

    public Map<Integer, DataFile> getFileMap() {
        synchronized (currentDataFile) {
            return new TreeMap<Integer, DataFile>(fileMap);
        }
    }

    public long getDiskSize() {
        return totalLength.get();
    }

    public void setReplicationTarget(ReplicationTarget replicationTarget) {
        this.replicationTarget = replicationTarget;
    }

    public ReplicationTarget getReplicationTarget() {
        return replicationTarget;
    }

    public String getFileSuffix() {
        return fileSuffix;
    }

    public void setFileSuffix(String fileSuffix) {
        this.fileSuffix = fileSuffix;
    }

    public boolean isChecksum() {
        return checksum;
    }

    public void setChecksum(boolean checksumWrites) {
        this.checksum = checksumWrites;
    }

    public boolean isCheckForCorruptionOnStartup() {
        return checkForCorruptionOnStartup;
    }

    public void setCheckForCorruptionOnStartup(boolean checkForCorruptionOnStartup) {
        this.checkForCorruptionOnStartup = checkForCorruptionOnStartup;
    }

    public void setWriteBatchSize(int writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
    }

    public int getWriteBatchSize() {
        return writeBatchSize;
    }

    public void setSizeAccumulator(AtomicLong storeSizeAccumulator) {
       this.totalLength = storeSizeAccumulator;
    }

    public void setEnableAsyncDiskSync(boolean val) {
        this.enableAsyncDiskSync = val;
    }

    public boolean isEnableAsyncDiskSync() {
        return enableAsyncDiskSync;
    }

    public JournalDiskSyncStrategy getJournalDiskSyncStrategy() {
        return journalDiskSyncStrategy;
    }

    public void setJournalDiskSyncStrategy(JournalDiskSyncStrategy journalDiskSyncStrategy) {
        this.journalDiskSyncStrategy = journalDiskSyncStrategy;
    }

    public boolean isJournalDiskSyncPeriodic() {
        return JournalDiskSyncStrategy.PERIODIC.equals(journalDiskSyncStrategy);
    }

    public void setDataFileRemovedListener(DataFileRemovedListener dataFileRemovedListener) {
        this.dataFileRemovedListener = dataFileRemovedListener;
    }

    public static class WriteCommand extends LinkedNode<WriteCommand> {
        public final Location location;
        public final ByteSequence data;
        final boolean sync;
        public final Runnable onComplete;

        public WriteCommand(Location location, ByteSequence data, boolean sync) {
            this.location = location;
            this.data = data;
            this.sync = sync;
            this.onComplete = null;
        }

        public WriteCommand(Location location, ByteSequence data, Runnable onComplete) {
            this.location = location;
            this.data = data;
            this.onComplete = onComplete;
            this.sync = false;
        }
    }

    public static class WriteKey {
        private final int file;
        private final long offset;
        private final int hash;

        public WriteKey(Location item) {
            file = item.getDataFileId();
            offset = item.getOffset();
            // TODO: see if we can build a better hash
            hash = (int)(file ^ offset);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof WriteKey) {
                WriteKey di = (WriteKey)obj;
                return di.file == file && di.offset == offset;
            }
            return false;
        }
    }
}
