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
package org.apache.activemq.store.kahadb.disk.page;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import org.apache.activemq.store.kahadb.disk.util.Sequence;
import org.apache.activemq.store.kahadb.disk.util.SequenceSet;
import org.apache.activemq.util.DataByteArrayOutputStream;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.LFUCache;
import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.RecoverableRandomAccessFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PageFile provides you random access to fixed sized disk pages. This object is not thread safe and therefore access to it should
 * be externally synchronized.
 * <p/>
 * The file has 3 parts:
 * Metadata Space: 4k : Reserved metadata area. Used to store persistent config about the file.
 * Recovery Buffer Space: Page Size * 1000 : This is a redo log used to prevent partial page writes from making the file inconsistent
 * Page Space: The pages in the page file.
 */
public class PageFile {

    private static final String PAGEFILE_SUFFIX = ".data";
    private static final String RECOVERY_FILE_SUFFIX = ".redo";
    private static final String FREE_FILE_SUFFIX = ".free";

    // 4k Default page size.
    public static final int DEFAULT_PAGE_SIZE = Integer.getInteger("defaultPageSize", 1024*4);
    public static final int DEFAULT_WRITE_BATCH_SIZE = Integer.getInteger("defaultWriteBatchSize", 1000);
    public static final int DEFAULT_PAGE_CACHE_SIZE = Integer.getInteger("defaultPageCacheSize", 100);;

    private static final int RECOVERY_FILE_HEADER_SIZE = 1024 * 4;
    //这个头是不是有点大啊 相当于一个头部信息有2k
    //是为了兼容pagesize的大小吗 但是pagesize是可以变得呀
    private static final int PAGE_FILE_HEADER_SIZE = 1024 * 4;

    // Recovery header is (long offset)
    private static final Logger LOG = LoggerFactory.getLogger(PageFile.class);

    // A PageFile will use a couple of files in this directory
    private final File directory;
    // And the file names in that directory will be based on this name.
    //文件的名字
    private final String name;

    // File handle used for reading pages..
    private RecoverableRandomAccessFile readFile;
    // File handle used for writing pages..
    private RecoverableRandomAccessFile writeFile;
    // File handle used for writing pages..
    //看起来好像之后缓存最后一次的批处理数据
    //里面记录的key是pageId
    private RecoverableRandomAccessFile recoveryFile;

    // The size of pages
    private int pageSize = DEFAULT_PAGE_SIZE;

    // The minimum number of space allocated to the recovery file in number of pages.
    private int recoveryFileMinPageCount = 1000;
    // The max size that we let the recovery file grow to.. ma exceed the max, but the file will get resize
    // to this max size as soon as  possible.
    private int recoveryFileMaxPageCount = 10000;
    // The number of pages in the current recovery buffer
    //记录这个干什么
    private int recoveryPageCount;

    private final AtomicBoolean loaded = new AtomicBoolean();
    // The number of pages we are aiming to write every time we
    // write to disk.
    //一批写多少个page
    int writeBatchSize = DEFAULT_WRITE_BATCH_SIZE;

    // We keep a cache of pages recently used?
    private Map<Long, Page> pageCache;
    // The cache of recently used pages.
    private boolean enablePageCaching = true;
    // How many pages will we keep in the cache?
    private int pageCacheSize = DEFAULT_PAGE_CACHE_SIZE;

    // Should first log the page write to the recovery buffer? Avoids partial
    // page write failures..
    private boolean enableRecoveryFile = true;
    // Will we sync writes to disk. Ensures that data will not be lost after a checkpoint()
    private boolean enableDiskSyncs = true;
    // Will writes be done in an async thread?
    private boolean enabledWriteThread = false;

    // These are used if enableAsyncWrites==true
    //表示是否需要停掉异步写的线程
    private final AtomicBoolean stopWriter = new AtomicBoolean();
    private Thread writerThread;
    private CountDownLatch checkpointLatch;

    // Keeps track of writes that are being written to disk.
    //key记录了什么信息 page id
    private final TreeMap<Long, PageWrite> writes = new TreeMap<Long, PageWrite>();

    // Keeps track of free pages.
    //下一个空闲的pageid 但是计算的时候没有管里面的文件里面的空闲page啊
    private final AtomicLong nextFreePageId = new AtomicLong();
    private SequenceSet freeList = new SequenceSet();

    //居然从元数据里面读
    //看来元数据存储的时候默认算一次事务
    private final AtomicLong nextTxid = new AtomicLong();

    // Persistent settings stored in the page file.
    private MetaData metaData;

    //这个又是在哪里加的数据呢
    //事务提交或则回滚的时候加进去的
    private final ArrayList<File> tmpFilesForRemoval = new ArrayList<File>();

    private boolean useLFRUEviction = false;
    private float LFUEvictionFactor = 0.2f;

    /**
     * Use to keep track of updated pages which have not yet been committed.
     */
    static class PageWrite {
        Page page;
        byte[] current;
        byte[] diskBound;
        long currentLocation = -1;
        long diskBoundLocation = -1;
        //当一个事务中需要写入的数据大于事务最大值时 数据就会写入临时文件
        File tmpFile;
        //文件中数据的长度 目前看来这个肯定刚好是pagesize的大小 那么这个变量存在的必要性是什么
        int length;

        public PageWrite(Page page, byte[] data) {
            this.page = page;
            current = data;
        }

        //currentLocation值是数据在文件中的偏移量
        //length值是文件中数据的大小
        public PageWrite(Page page, long currentLocation, int length, File tmpFile) {
            this.page = page;
            this.currentLocation = currentLocation;
            this.tmpFile = tmpFile;
            this.length = length;
        }

        public void setCurrent(Page page, byte[] data) {
            this.page = page;
            current = data;
            currentLocation = -1;
            diskBoundLocation = -1;
        }

        public void setCurrentLocation(Page page, long location, int length) {
            this.page = page;
            this.currentLocation = location;
            this.length = length;
            this.current = null;
        }

        @Override
        public String toString() {
            return "[PageWrite:" + page.getPageId() + "-" + page.getType() + "]";
        }

        @SuppressWarnings("unchecked")
        public Page getPage() {
            return page;
        }

        public byte[] getDiskBound() throws IOException {
            if (diskBound == null && diskBoundLocation != -1) {
                diskBound = new byte[length];
                try(RandomAccessFile file = new RandomAccessFile(tmpFile, "r")) {
                    file.seek(diskBoundLocation);
                    file.read(diskBound);
                }
                diskBoundLocation = -1;
            }
            return diskBound;
        }

        void begin() {
            //说明临时文件有东西
            if (currentLocation != -1) {
                diskBoundLocation = currentLocation;
            } else {//说明临时文件没东西  数据是在current里面的
                diskBound = current;
            }
            current = null;
            currentLocation = -1;
        }

        /**
         * @return true if there is no pending writes to do.
         */
        boolean done() {
            diskBoundLocation = -1;
            diskBound = null;
            return current == null || currentLocation == -1;
        }

        boolean isDone() {
            return diskBound == null && diskBoundLocation == -1 && current == null && currentLocation == -1;
        }
    }

    /**
     * The MetaData object hold the persistent data associated with a PageFile object.
     */
    public static class MetaData {

        //存的是class的名字
        String fileType;
        //现在还是第一版
        String fileTypeVersion;

        //应该记录的是事务的id吧
        //保存的时候这个值有加1
        long metaDataTxId = -1;
        int pageSize;
        //看名字应该是关闭的时候清空
        //应该是关闭的时候会做一些清理工作
        //启动的时候会设为false 是表示还没做清理工作吧
        boolean cleanShutdown;
        //应该存的是上一个事务id
        long lastTxId;
        //没有使用的page数量 难道是想复用
        long freePages;

        public String getFileType() {
            return fileType;
        }

        public void setFileType(String fileType) {
            this.fileType = fileType;
        }

        public String getFileTypeVersion() {
            return fileTypeVersion;
        }

        public void setFileTypeVersion(String version) {
            this.fileTypeVersion = version;
        }

        public long getMetaDataTxId() {
            return metaDataTxId;
        }

        public void setMetaDataTxId(long metaDataTxId) {
            this.metaDataTxId = metaDataTxId;
        }

        public int getPageSize() {
            return pageSize;
        }

        public void setPageSize(int pageSize) {
            this.pageSize = pageSize;
        }

        public boolean isCleanShutdown() {
            return cleanShutdown;
        }

        public void setCleanShutdown(boolean cleanShutdown) {
            this.cleanShutdown = cleanShutdown;
        }

        public long getLastTxId() {
            return lastTxId;
        }

        public void setLastTxId(long lastTxId) {
            this.lastTxId = lastTxId;
        }

        public long getFreePages() {
            return freePages;
        }

        public void setFreePages(long value) {
            this.freePages = value;
        }
    }

    public Transaction tx() {
        assertLoaded();
        return new Transaction(this);
    }

    /**
     * Creates a PageFile in the specified directory who's data files are named by name.
     */
    public PageFile(File directory, String name) {
        this.directory = directory;
        this.name = name;
    }

    /**
     * Deletes the files used by the PageFile object.  This method can only be used when this object is not loaded.
     *
     * @throws IOException           if the files cannot be deleted.
     * @throws IllegalStateException if this PageFile is loaded
     */
    public void delete() throws IOException {
        if (loaded.get()) {
            throw new IllegalStateException("Cannot delete page file data when the page file is loaded");
        }
        delete(getMainPageFile());
        delete(getFreeFile());
        delete(getRecoveryFile());
    }

    public void archive() throws IOException {
        if (loaded.get()) {
            throw new IllegalStateException("Cannot delete page file data when the page file is loaded");
        }
        long timestamp = System.currentTimeMillis();
        archive(getMainPageFile(), String.valueOf(timestamp));
        archive(getFreeFile(), String.valueOf(timestamp));
        archive(getRecoveryFile(), String.valueOf(timestamp));
    }

    /**
     * @param file
     * @throws IOException
     */
    private void delete(File file) throws IOException {
        if (file.exists() && !file.delete()) {
            throw new IOException("Could not delete: " + file.getPath());
        }
    }

    private void archive(File file, String suffix) throws IOException {
        if (file.exists()) {
            File archive = new File(file.getPath() + "-" + suffix);
            if (!file.renameTo(archive)) {
                throw new IOException("Could not archive: " + file.getPath() + " to " + file.getPath());
            }
        }
    }

    /**
     * Loads the page file so that it can be accessed for read/write purposes.  This allocates OS resources.  If this is the
     * first time the page file is loaded, then this creates the page file in the file system.
     *
     * @throws IOException           If the page file cannot be loaded. This could be cause the existing page file is corrupt is a bad version or if
     *                               there was a disk error.
     * @throws IllegalStateException If the page file was already loaded.
     */
    public void load() throws IOException, IllegalStateException {
        if (loaded.compareAndSet(false, true)) {

            if (enablePageCaching) {
                if (isUseLFRUEviction()) {
                    //根据使用次数弹出的缓存
                    pageCache = Collections.synchronizedMap(new LFUCache<Long, Page>(pageCacheSize, getLFUEvictionFactor()));
                } else {
                    //根据最后访问时间弹出的缓存
                    pageCache = Collections.synchronizedMap(new LRUCache<Long, Page>(pageCacheSize, pageCacheSize, 0.75f, true));
                }
            }

            //按照这个感觉应该是一个目录下可以配置多个index文件 但是pagefile好像只有一个
            File file = getMainPageFile();
            IOHelper.mkdirs(file.getParentFile());
            writeFile = new RecoverableRandomAccessFile(file, "rw", false);
            readFile = new RecoverableRandomAccessFile(file, "r");

            if (readFile.length() > 0) {//文件有数据就从里面读取元数据
                // Load the page size setting cause that can't change once the file is created.
                loadMetaData();
                //还能被覆盖的吗
                pageSize = metaData.getPageSize();
            } else {
                // Store the page size setting cause that can't change once the file is created.
                metaData = new MetaData();
                metaData.setFileType(PageFile.class.getName());
                metaData.setFileTypeVersion("1");
                metaData.setPageSize(getPageSize());
                metaData.setCleanShutdown(true);
                metaData.setFreePages(-1);
                metaData.setLastTxId(0);
                storeMetaData();
            }

            if (enableRecoveryFile) {
                recoveryFile = new RecoverableRandomAccessFile(getRecoveryFile(), "rw");
            }

            boolean needsFreePageRecovery = false;

            if (metaData.isCleanShutdown()) {
                nextTxid.set(metaData.getLastTxId() + 1);
                if (metaData.getFreePages() > 0) {
                    //这读的时候也没看是不是个这个值相等啊
                    loadFreeList();
                }
            } else {
                LOG.debug(toString() + ", Recovering page file...");
                //做恢复工作时顺便找到下一个事务的id
                nextTxid.set(redoRecoveryUpdates());
                //因为还没做过这个操作
                needsFreePageRecovery = true;
            }

            if (writeFile.length() < PAGE_FILE_HEADER_SIZE) {
                writeFile.setLength(PAGE_FILE_HEADER_SIZE);
            }
            nextFreePageId.set((writeFile.length() - PAGE_FILE_HEADER_SIZE) / pageSize);

            //这么简单粗暴吗  直接扫面了整个index找到类型为空闲的page
            if (needsFreePageRecovery) {
                // Scan all to find the free pages after nextFreePageId is set
                freeList = new SequenceSet();
                for (Iterator<Page> i = tx().iterator(true); i.hasNext(); ) {
                    Page page = i.next();
                    if (page.getType() == Page.PAGE_FREE_TYPE) {
                        freeList.add(page.getPageId());
                    }
                }
            }

            metaData.setCleanShutdown(false);
            storeMetaData();
            getFreeFile().delete();
            startWriter();
        } else {
            throw new IllegalStateException("Cannot load the page file when it is already loaded.");
        }
    }


    /**
     * Unloads a previously loaded PageFile.  This deallocates OS related resources like file handles.
     * once unloaded, you can no longer use the page file to read or write Pages.
     *
     * @throws IOException           if there was a disk error occurred while closing the down the page file.
     * @throws IllegalStateException if the PageFile is not loaded
     */
    public void unload() throws IOException {
        if (loaded.compareAndSet(true, false)) {
            flush();
            try {
                stopWriter();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }

            if (freeList.isEmpty()) {
                metaData.setFreePages(0);
            } else {
                storeFreeList();
                metaData.setFreePages(freeList.size());
            }

            metaData.setLastTxId(nextTxid.get() - 1);
            metaData.setCleanShutdown(true);
            storeMetaData();

            if (readFile != null) {
                readFile.close();
                readFile = null;
                writeFile.close();
                writeFile = null;
                if (enableRecoveryFile) {
                    recoveryFile.close();
                    recoveryFile = null;
                }
                freeList.clear();
                if (pageCache != null) {
                    pageCache = null;
                }
                synchronized (writes) {
                    writes.clear();
                }
            }
        } else {
            throw new IllegalStateException("Cannot unload the page file when it is not loaded");
        }
    }

    public boolean isLoaded() {
        return loaded.get();
    }

    public void allowIOResumption() {
        loaded.set(true);
    }

    /**
     * Flush and sync all write buffers to disk.
     *
     * @throws IOException If an disk error occurred.
     */
    public void flush() throws IOException {

        if (enabledWriteThread && stopWriter.get()) {
            throw new IOException("Page file already stopped: checkpointing is not allowed");
        }

        // Setup a latch that gets notified when all buffered writes hits the disk.
        CountDownLatch checkpointLatch;
        synchronized (writes) {
            if (writes.isEmpty()) {
                return;
            }
            if (enabledWriteThread) {
                if (this.checkpointLatch == null) {
                    this.checkpointLatch = new CountDownLatch(1);
                }
                checkpointLatch = this.checkpointLatch;
                writes.notify();
            } else {
                writeBatch();
                return;
            }
        }
        try {
            checkpointLatch.await();
        } catch (InterruptedException e) {
            InterruptedIOException ioe = new InterruptedIOException();
            ioe.initCause(e);
            throw ioe;
        }
    }


    @Override
    public String toString() {
        return "Page File: " + getMainPageFile();
    }

    ///////////////////////////////////////////////////////////////////
    // Private Implementation Methods
    ///////////////////////////////////////////////////////////////////
    private File getMainPageFile() {
        return new File(directory, IOHelper.toFileSystemSafeName(name) + PAGEFILE_SUFFIX);
    }

    public File getFreeFile() {
        return new File(directory, IOHelper.toFileSystemSafeName(name) + FREE_FILE_SUFFIX);
    }

    public File getRecoveryFile() {
        return new File(directory, IOHelper.toFileSystemSafeName(name) + RECOVERY_FILE_SUFFIX);
    }

    public long toOffset(long pageId) {
        return PAGE_FILE_HEADER_SIZE + (pageId * pageSize);
    }

    private void loadMetaData() throws IOException {

        ByteArrayInputStream is;
        MetaData v1 = new MetaData();
        MetaData v2 = new MetaData();
        try {
            Properties p = new Properties();
            byte[] d = new byte[PAGE_FILE_HEADER_SIZE / 2];
            readFile.seek(0);
            readFile.readFully(d);
            is = new ByteArrayInputStream(d);
            p.load(is);
            IntrospectionSupport.setProperties(v1, p);
        } catch (IOException e) {
            v1 = null;
        }

        try {
            Properties p = new Properties();
            byte[] d = new byte[PAGE_FILE_HEADER_SIZE / 2];
            readFile.seek(PAGE_FILE_HEADER_SIZE / 2);
            readFile.readFully(d);
            is = new ByteArrayInputStream(d);
            p.load(is);
            IntrospectionSupport.setProperties(v2, p);
        } catch (IOException e) {
            v2 = null;
        }

        //允许一个出错啊
        if (v1 == null && v2 == null) {
            throw new IOException("Could not load page file meta data");
        }

        //感觉很简单粗暴啊
        if (v1 == null || v1.metaDataTxId < 0) {
            metaData = v2;
        } else if (v2 == null || v1.metaDataTxId < 0) {
            metaData = v1;
        } else if (v1.metaDataTxId == v2.metaDataTxId) {
            metaData = v1; // use the first since the 2nd could be a partial..
        } else {
            metaData = v2; // use the second cause the first is probably a partial.
        }
    }

    private void storeMetaData() throws IOException {
        // Convert the metadata into a property format
        metaData.metaDataTxId++;
        Properties p = new Properties();
        IntrospectionSupport.getProperties(metaData, p, null);

        ByteArrayOutputStream os = new ByteArrayOutputStream(PAGE_FILE_HEADER_SIZE);
        p.store(os, "");
        if (os.size() > PAGE_FILE_HEADER_SIZE / 2) {
            throw new IOException("Configuation is larger than: " + PAGE_FILE_HEADER_SIZE / 2);
        }
        // Fill the rest with space...
        byte[] filler = new byte[(PAGE_FILE_HEADER_SIZE / 2) - os.size()];
        Arrays.fill(filler, (byte) ' ');
        os.write(filler);
        os.flush();

        byte[] d = os.toByteArray();

        // So we don't loose it.. write it 2 times...
        //同一份数据写两次 读的时候还会不用也是奇怪
        writeFile.seek(0);
        writeFile.write(d);
        writeFile.sync();
        writeFile.seek(PAGE_FILE_HEADER_SIZE / 2);
        writeFile.write(d);
        writeFile.sync();
    }

    private void storeFreeList() throws IOException {
        FileOutputStream os = new FileOutputStream(getFreeFile());
        DataOutputStream dos = new DataOutputStream(os);
        SequenceSet.Marshaller.INSTANCE.writePayload(freeList, dos);
        dos.close();
    }

    private void loadFreeList() throws IOException {
        freeList.clear();
        FileInputStream is = new FileInputStream(getFreeFile());
        DataInputStream dis = new DataInputStream(is);
        freeList = SequenceSet.Marshaller.INSTANCE.readPayload(dis);
        dis.close();
    }

    ///////////////////////////////////////////////////////////////////
    // Property Accessors
    ///////////////////////////////////////////////////////////////////

    /**
     * Is the recovery buffer used to double buffer page writes.  Enabled by default.
     *
     * @return is the recovery buffer enabled.
     */
    public boolean isEnableRecoveryFile() {
        return enableRecoveryFile;
    }

    /**
     * Sets if the recovery buffer uses to double buffer page writes.  Enabled by default.  Disabling this
     * may potentially cause partial page writes which can lead to page file corruption.
     */
    public void setEnableRecoveryFile(boolean doubleBuffer) {
        assertNotLoaded();
        this.enableRecoveryFile = doubleBuffer;
    }

    /**
     * @return Are page writes synced to disk?
     */
    public boolean isEnableDiskSyncs() {
        return enableDiskSyncs;
    }

    /**
     * Allows you enable syncing writes to disk.
     */
    public void setEnableDiskSyncs(boolean syncWrites) {
        assertNotLoaded();
        this.enableDiskSyncs = syncWrites;
    }

    /**
     * @return the page size
     */
    public int getPageSize() {
        return this.pageSize;
    }

    /**
     * @return the amount of content data that a page can hold.
     */
    public int getPageContentSize() {
        return this.pageSize - Page.PAGE_HEADER_SIZE;
    }

    /**
     * Configures the page size used by the page file.  By default it is 4k.  Once a page file is created on disk,
     * subsequent loads of that file will use the original pageSize.  Once the PageFile is loaded, this setting
     * can no longer be changed.
     *
     * @param pageSize the pageSize to set
     * @throws IllegalStateException once the page file is loaded.
     */
    public void setPageSize(int pageSize) throws IllegalStateException {
        assertNotLoaded();
        this.pageSize = pageSize;
    }

    /**
     * @return true if read page caching is enabled
     */
    public boolean isEnablePageCaching() {
        return this.enablePageCaching;
    }

    /**
     * @param enablePageCaching allows you to enable read page caching
     */
    public void setEnablePageCaching(boolean enablePageCaching) {
        assertNotLoaded();
        this.enablePageCaching = enablePageCaching;
    }

    /**
     * @return the maximum number of pages that will get stored in the read page cache.
     */
    public int getPageCacheSize() {
        return this.pageCacheSize;
    }

    /**
     * @param pageCacheSize Sets the maximum number of pages that will get stored in the read page cache.
     */
    public void setPageCacheSize(int pageCacheSize) {
        assertNotLoaded();
        this.pageCacheSize = pageCacheSize;
    }

    public boolean isEnabledWriteThread() {
        return enabledWriteThread;
    }

    public void setEnableWriteThread(boolean enableAsyncWrites) {
        assertNotLoaded();
        this.enabledWriteThread = enableAsyncWrites;
    }

    public long getDiskSize() throws IOException {
        return toOffset(nextFreePageId.get());
    }

    /**
     * @return the number of pages allocated in the PageFile
     */
    public long getPageCount() {
        return nextFreePageId.get();
    }

    public int getRecoveryFileMinPageCount() {
        return recoveryFileMinPageCount;
    }

    public long getFreePageCount() {
        assertLoaded();
        return freeList.rangeSize();
    }

    public void setRecoveryFileMinPageCount(int recoveryFileMinPageCount) {
        assertNotLoaded();
        this.recoveryFileMinPageCount = recoveryFileMinPageCount;
    }

    public int getRecoveryFileMaxPageCount() {
        return recoveryFileMaxPageCount;
    }

    public void setRecoveryFileMaxPageCount(int recoveryFileMaxPageCount) {
        assertNotLoaded();
        this.recoveryFileMaxPageCount = recoveryFileMaxPageCount;
    }

    public int getWriteBatchSize() {
        return writeBatchSize;
    }

    public void setWriteBatchSize(int writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
    }

    public float getLFUEvictionFactor() {
        return LFUEvictionFactor;
    }

    public void setLFUEvictionFactor(float LFUEvictionFactor) {
        this.LFUEvictionFactor = LFUEvictionFactor;
    }

    public boolean isUseLFRUEviction() {
        return useLFRUEviction;
    }

    public void setUseLFRUEviction(boolean useLFRUEviction) {
        this.useLFRUEviction = useLFRUEviction;
    }

    ///////////////////////////////////////////////////////////////////
    // Package Protected Methods exposed to Transaction
    ///////////////////////////////////////////////////////////////////

    /**
     * @throws IllegalStateException if the page file is not loaded.
     */
    void assertLoaded() throws IllegalStateException {
        if (!loaded.get()) {
            throw new IllegalStateException("PageFile is not loaded");
        }
    }

    void assertNotLoaded() throws IllegalStateException {
        if (loaded.get()) {
            throw new IllegalStateException("PageFile is loaded");
        }
    }

    /**
     * Allocates a block of free pages that you can write data to.
     *
     * @param count the number of sequential pages to allocate
     * @return the first page of the sequential set.
     * @throws IOException           If an disk error occurred.
     * @throws IllegalStateException if the PageFile is not loaded
     */
    <T> Page<T> allocate(int count) throws IOException {
        assertLoaded();
        if (count <= 0) {
            throw new IllegalArgumentException("The allocation count must be larger than zero");
        }

        Sequence seq = freeList.removeFirstSequence(count);

        // We may need to create new free pages...
        if (seq == null) {

            Page<T> first = null;
            int c = count;

            // Perform the id's only once....
            long pageId = nextFreePageId.getAndAdd(count);
            long writeTxnId = nextTxid.getAndAdd(count);

            while (c-- > 0) {
                Page<T> page = new Page<T>(pageId++);
                page.makeFree(writeTxnId++);

                if (first == null) {
                    first = page;
                }

                addToCache(page);
                DataByteArrayOutputStream out = new DataByteArrayOutputStream(pageSize);
                page.write(out);
                write(page, out.getData());

                // LOG.debug("allocate writing: "+page.getPageId());
            }

            return first;
        }

        Page<T> page = new Page<T>(seq.getFirst());
        //为什么要把它设为0
        page.makeFree(0);
        // LOG.debug("allocated: "+page.getPageId());
        return page;
    }

    long getNextWriteTransactionId() {
        return nextTxid.incrementAndGet();
    }

    synchronized void readPage(long pageId, byte[] data) throws IOException {
        readFile.seek(toOffset(pageId));
        readFile.readFully(data);
    }

    public void freePage(long pageId) {
        freeList.add(pageId);
        removeFromCache(pageId);
    }

    @SuppressWarnings("unchecked")
    private <T> void write(Page<T> page, byte[] data) throws IOException {
        final PageWrite write = new PageWrite(page, data);
        Entry<Long, PageWrite> entry = new Entry<Long, PageWrite>() {
            @Override
            public Long getKey() {
                return write.getPage().getPageId();
            }

            @Override
            public PageWrite getValue() {
                return write;
            }

            @Override
            public PageWrite setValue(PageWrite value) {
                return null;
            }
        };
        Entry<Long, PageWrite>[] entries = new Map.Entry[]{entry};
        write(Arrays.asList(entries));
    }

    void write(Collection<Map.Entry<Long, PageWrite>> updates) throws IOException {
        synchronized (writes) {
            if (enabledWriteThread) {
                while (writes.size() >= writeBatchSize && !stopWriter.get()) {
                    try {
                        writes.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new InterruptedIOException();
                    }
                }
            }

            boolean longTx = false;

            for (Map.Entry<Long, PageWrite> entry : updates) {
                Long key = entry.getKey();
                PageWrite value = entry.getValue();
                PageWrite write = writes.get(key);
                if (write == null) {
                    writes.put(key, value);
                } else {
                    if (value.currentLocation != -1) {
                        write.setCurrentLocation(value.page, value.currentLocation, value.length);
                        write.tmpFile = value.tmpFile;
                        longTx = true;
                    } else {
                        write.setCurrent(value.page, value.current);
                    }
                }
            }

            // Once we start approaching capacity, notify the writer to start writing
            // sync immediately for long txs
            //longTx表示的是数据在文件里面 也就是数据量很大
            if (longTx || canStartWriteBatch()) {

                if (enabledWriteThread) {
                    writes.notify();
                } else {
                    writeBatch();
                }
            }
        }
    }

    private boolean canStartWriteBatch() {
        //当前百分比
        int capacityUsed = ((writes.size() * 100) / writeBatchSize);
        if (enabledWriteThread) {
            // The constant 10 here controls how soon write batches start going to disk..
            // would be nice to figure out how to auto tune that value.  Make to small and
            // we reduce through put because we are locking the write mutex too often doing writes
            //checkpointLatch!=null是不是说明有线程在等待执行完成 所以需要立即执行
            return capacityUsed >= 10 || checkpointLatch != null;
        } else {
            return capacityUsed >= 80 || checkpointLatch != null;
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Cache Related operations
    ///////////////////////////////////////////////////////////////////
    @SuppressWarnings("unchecked")
    <T> Page<T> getFromCache(long pageId) {
        synchronized (writes) {
            PageWrite pageWrite = writes.get(pageId);
            if (pageWrite != null) {
                return pageWrite.page;
            }
        }

        Page<T> result = null;
        if (enablePageCaching) {
            result = pageCache.get(pageId);
        }
        return result;
    }

    void addToCache(Page page) {
        if (enablePageCaching) {
            pageCache.put(page.getPageId(), page);
        }
    }

    void removeFromCache(long pageId) {
        if (enablePageCaching) {
            pageCache.remove(pageId);
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Internal Double write implementation follows...
    ///////////////////////////////////////////////////////////////////

    private void pollWrites() {
        try {
            while (!stopWriter.get()) {
                // Wait for a notification...
                synchronized (writes) {
                    writes.notifyAll();

                    // If there is not enough to write, wait for a notification...
                    //为什么要加个判断条件 checkpointLatch==null
                    //感觉是不是只要有数据加进来这个一定不等于null
                    while (writes.isEmpty() && checkpointLatch == null && !stopWriter.get()) {
                        writes.wait(100);
                    }

                    if (writes.isEmpty()) {
                        releaseCheckpointWaiter();
                    }
                }
                writeBatch();
            }
        } catch (Throwable e) {
            LOG.info("An exception was raised while performing poll writes", e);
        } finally {
            releaseCheckpointWaiter();
        }
    }

    //有数据了 开始写
    private void writeBatch() throws IOException {

        CountDownLatch checkpointLatch;
        ArrayList<PageWrite> batch;
        synchronized (writes) {
            // If there is not enough to write, wait for a notification...

            batch = new ArrayList<PageWrite>(writes.size());
            // build a write batch from the current write cache.
            for (PageWrite write : writes.values()) {
                batch.add(write);
                // Move the current write to the diskBound write, this lets folks update the
                // page again without blocking for this write.
                write.begin();
                //这个判断说明这个write是没有数据的
                if (write.diskBound == null && write.diskBoundLocation == -1) {
                    batch.remove(write);
                }
            }

            // Grab on to the existing checkpoint latch cause once we do this write we can
            // release the folks that were waiting for those writes to hit disk.
            checkpointLatch = this.checkpointLatch;
            this.checkpointLatch = null;
        }

        try {

            // First land the writes in the recovery file
            if (enableRecoveryFile) {
                Checksum checksum = new Adler32();

                //这不就是每次新的一批数据会覆盖旧的一批吗
                recoveryFile.seek(RECOVERY_FILE_HEADER_SIZE);

                for (PageWrite w : batch) {
                    try {
                        checksum.update(w.getDiskBound(), 0, pageSize);
                    } catch (Throwable t) {
                        throw IOExceptionSupport.create("Cannot create recovery file. Reason: " + t, t);
                    }
                    recoveryFile.writeLong(w.page.getPageId());
                    recoveryFile.write(w.getDiskBound(), 0, pageSize);
                }

                // Can we shrink the recovery buffer??
                if (recoveryPageCount > recoveryFileMaxPageCount) {
                    int t = Math.max(recoveryFileMinPageCount, batch.size());
                    recoveryFile.setLength(recoveryFileSizeForPages(t));
                }

                // Record the page writes in the recovery buffer.
                recoveryFile.seek(0);
                // Store the next tx id...
                recoveryFile.writeLong(nextTxid.get());
                // Store the checksum for thw write batch so that on recovery we
                // know if we have a consistent
                // write batch on disk.
                recoveryFile.writeLong(checksum.getValue());
                // Write the # of pages that will follow
                recoveryFile.writeInt(batch.size());

                if (enableDiskSyncs) {
                    recoveryFile.sync();
                }
            }

            for (PageWrite w : batch) {
                writeFile.seek(toOffset(w.page.getPageId()));
                writeFile.write(w.getDiskBound(), 0, pageSize);
                w.done();
            }

            if (enableDiskSyncs) {
                writeFile.sync();
            }

        } catch (IOException ioError) {
            LOG.info("Unexpected io error on pagefile write of " + batch.size() + " pages.", ioError);
            // any subsequent write needs to be prefaced with a considered call to redoRecoveryUpdates
            // to ensure disk image is self consistent
            loaded.set(false);
            throw  ioError;
        } finally {
            synchronized (writes) {
                for (PageWrite w : batch) {
                    // If there are no more pending writes, then remove it from
                    // the write cache.
                    if (w.isDone()) {
                        writes.remove(w.page.getPageId());
                        if (w.tmpFile != null && tmpFilesForRemoval.contains(w.tmpFile)) {
                            if (!w.tmpFile.delete()) {
                                throw new IOException("Can't delete temporary KahaDB transaction file:" + w.tmpFile);
                            }
                            tmpFilesForRemoval.remove(w.tmpFile);
                        }
                    }
                }
            }

            if (checkpointLatch != null) {
                checkpointLatch.countDown();
            }
        }
    }

    public void removeTmpFile(File file) {
        tmpFilesForRemoval.add(file);
    }

    private long recoveryFileSizeForPages(int pageCount) {
        //8字节用来干什么
        //八个字节用来记录page id
        return RECOVERY_FILE_HEADER_SIZE + ((pageSize + 8) * pageCount);
    }

    private void releaseCheckpointWaiter() {
        if (checkpointLatch != null) {
            checkpointLatch.countDown();
            checkpointLatch = null;
        }
    }

    /**
     * Inspects the recovery buffer and re-applies any
     * partially applied page writes.
     *
     * @return the next transaction id that can be used.
     */
    private long redoRecoveryUpdates() throws IOException {
        if (!enableRecoveryFile) {
            return 0;
        }
        recoveryPageCount = 0;

        // Are we initializing the recovery file?
        if (recoveryFile.length() == 0) {
            // Write an empty header..
            recoveryFile.write(new byte[RECOVERY_FILE_HEADER_SIZE]);
            // Preallocate the minium size for better performance.
            recoveryFile.setLength(recoveryFileSizeForPages(recoveryFileMinPageCount));
            return 0;
        }

        // How many recovery pages do we have in the recovery buffer?
        recoveryFile.seek(0);
        //那么长的头就记录了这三个信息
        long nextTxId = recoveryFile.readLong();
        long expectedChecksum = recoveryFile.readLong();
        int pageCounter = recoveryFile.readInt();

        recoveryFile.seek(RECOVERY_FILE_HEADER_SIZE);
        Checksum checksum = new Adler32();
        LinkedHashMap<Long, byte[]> batch = new LinkedHashMap<Long, byte[]>();
        try {
            for (int i = 0; i < pageCounter; i++) {
                //记录的是page id
                long offset = recoveryFile.readLong();
                byte[] data = new byte[pageSize];
                if (recoveryFile.read(data, 0, pageSize) != pageSize) {
                    // Invalid recovery record, Could not fully read the data". Probably due to a partial write to the recovery buffer
                    //恢复不了也没做什么
                    return nextTxId;
                }
                checksum.update(data, 0, pageSize);
                batch.put(offset, data);
            }
        } catch (Exception e) {
            // If an error occurred it was cause the redo buffer was not full written out correctly.. so don't redo it.
            // as the pages should still be consistent.
            LOG.debug("Redo buffer was not fully intact: ", e);
            return nextTxId;
        }

        recoveryPageCount = pageCounter;

        // If the checksum is not valid then the recovery buffer was partially written to disk.
        if (checksum.getValue() != expectedChecksum) {
            return nextTxId;
        }

        // Re-apply all the writes in the recovery buffer.
        for (Map.Entry<Long, byte[]> e : batch.entrySet()) {
            writeFile.seek(toOffset(e.getKey()));
            writeFile.write(e.getValue());
        }

        // And sync it to disk
        writeFile.sync();
        return nextTxId;
    }

    //开始异步写数据 里面放的key是数据在datafile的偏移量 值是数据本身
    private void startWriter() {
        synchronized (writes) {
            //如果不允许异步写数据就什么都没做
            if (enabledWriteThread) {
                stopWriter.set(false);
                writerThread = new Thread("KahaDB Page Writer") {
                    @Override
                    public void run() {
                        pollWrites();
                    }
                };
                writerThread.setPriority(Thread.MAX_PRIORITY);
                writerThread.setDaemon(true);
                writerThread.start();
            }
        }
    }

    private void stopWriter() throws InterruptedException {
        if (enabledWriteThread) {
            stopWriter.set(true);
            writerThread.join();
        }
    }

    public File getFile() {
        return getMainPageFile();
    }

    public File getDirectory() {
        return directory;
    }
}
