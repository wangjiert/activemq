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
package org.apache.activemq.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Date;

/**
 * Used to lock a File.
 *
 * @author chirino
 */
public class LockFile {

    //为什么选择用这个属性来控制是否支持文件锁
    private static final boolean DISABLE_FILE_LOCK = Boolean.getBoolean("java.nio.channels.FileLock.broken");
    final private File file;
    //记录锁文件最后修改的时间
    private long lastModified;

    private FileLock lock;
    private RandomAccessFile randomAccessLockFile;
    //被锁了几次
    private int lockCounter;
    private final boolean deleteOnUnlock;
    //表示文件是否被锁住
    private volatile boolean locked;
    private String lockSystemPropertyName = "";

    private static final Logger LOG = LoggerFactory.getLogger(LockFile.class);

    public LockFile(File file, boolean deleteOnUnlock) {
        this.file = file;
        this.deleteOnUnlock = deleteOnUnlock;
    }

    /**
     * @throws IOException
     */
    synchronized public void lock() throws IOException {
        //如果不允许文件锁就直接返回
        if (DISABLE_FILE_LOCK) {
            return;
        }

        //这个表示的是文件被锁的次数吗
        if (lockCounter > 0) {
            return;
        }

        //创建上一级目录
        IOHelper.mkdirs(file.getParentFile());
        synchronized (LockFile.class) {
            //只是为了创建一个key吧
            lockSystemPropertyName = getVmLockKey();
            //值是时间
            //是通过这里实现的加锁吗 如果已经有锁了就直接报错
            //看来这个只是防止同一时间多次创建文件锁
            if (System.getProperty(lockSystemPropertyName) != null) {
                throw new IOException("File '" + file + "' could not be locked as lock is already held for this jvm. Value: " + System.getProperty(lockSystemPropertyName));
            }
            System.setProperty(lockSystemPropertyName, new Date().toString());
        }
        try {
            if (lock == null) {
                randomAccessLockFile = new RandomAccessFile(file, "rw");
                IOException reason = null;
                try {
                    //如果说这个锁是被虚拟机持有的 那么这个锁有用吗
                    //锁被虚拟机持有是什么意思呢 这个对象可以跨线程的被访问的意思吗 如果已经锁住了再调用一次是什么效果
                    //我感觉是对一个虚拟机而言 这个锁获得了就是所有线程都获得了  其他虚拟机或者语言的代码不能在获得
                    lock = randomAccessLockFile.getChannel().tryLock(0, Math.max(1, randomAccessLockFile.getChannel().size()), false);
                } catch (OverlappingFileLockException e) {
                    reason = IOExceptionSupport.create("File '" + file + "' could not be locked.", e);
                } catch (IOException ioe) {
                    reason = ioe;
                }
                //这里逻辑有点厉害啊 报错之后什么都没干啊 只是记录了原因 如果这个虚拟机之前就获得了锁那么 这个错误直接被忽略了
                //表示拿到了锁
                if (lock != null) {
                    //track lastModified only if we are able to successfully obtain the lock.
                    randomAccessLockFile.writeLong(System.currentTimeMillis());
                    randomAccessLockFile.getChannel().force(true);
                    lastModified = file.lastModified();
                    lockCounter++;
                    //开始的时候就写入了时间值 现在又来一次 这个值有什么用
                    //之前设置值的时候进行了同步锁 现在有不进行同步锁 为什么
                    System.setProperty(lockSystemPropertyName, new Date().toString());
                    locked = true;
                } else { //已经被别人锁了
                    // new read file for next attempt
                    closeReadFile();
                    if (reason != null) {
                        throw reason;
                    }
                    throw new IOException("File '" + file + "' could not be locked.");
                }

            }
        } finally {
            synchronized (LockFile.class) {
                //只要拿到了锁就不会情况这个东西
                //这里要测试一下重复拿锁是个什么情况 我感觉还是会返回锁对象 还是同一个对象
                //验证结果: 就算同一个虚拟机两次去获取文件锁也会报错
                if (lock == null) {
                    System.getProperties().remove(lockSystemPropertyName);
                }
            }
        }
    }

    /**
     */
    synchronized public void unlock() {
        if (DISABLE_FILE_LOCK) {
            return;
        }

        lockCounter--;
        if (lockCounter != 0) {
            return;
        }

        // release the lock..
        if (lock != null) {
            try {
                lock.release();
            } catch (Throwable ignore) {
            } finally {
                if (lockSystemPropertyName != null) {
                    System.getProperties().remove(lockSystemPropertyName);
                }
                lock = null;
            }
        }
        closeReadFile();

        if (locked && deleteOnUnlock) {
            file.delete();
        }
    }

    private String getVmLockKey() throws IOException {
        return getClass().getName() + ".lock." + file.getCanonicalPath();
    }

    private void closeReadFile() {
        // close the file.
        if (randomAccessLockFile != null) {
            try {
                randomAccessLockFile.close();
            } catch (Throwable ignore) {
            }
            randomAccessLockFile = null;
        }
    }

    /**
     * @return true if the lock file's last modified does not match the locally cached lastModified, false otherwise
     */
    private boolean hasBeenModified() {
        boolean modified = false;

        //Create a new instance of the File object so we can get the most up to date information on the file.
        File localFile = new File(file.getAbsolutePath());

        if (localFile.exists()) {
            if(localFile.lastModified() != lastModified) {
                LOG.info("Lock file " + file.getAbsolutePath() + ", locked at " + new Date(lastModified) + ", has been modified at " + new Date(localFile.lastModified()));
                modified = true;
            }
        }
        else {
            //The lock file is missing
            LOG.info("Lock file " + file.getAbsolutePath() + ", does not exist");
            modified = true;
        }

        return modified;
    }

    public boolean keepAlive() {
        locked = locked && lock != null && lock.isValid() && !hasBeenModified();
        return locked;
    }

}
