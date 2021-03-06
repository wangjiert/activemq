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
package org.apache.activemq.broker.region.cursors;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageRecoveryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Store based cursor
 *
 */
public abstract class AbstractStoreCursor extends AbstractPendingMessageCursor implements MessageRecoveryListener {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractStoreCursor.class);
    protected final Destination regionDestination;
    //已经读出来的消息吗
    //看来消息应该首先从这里面拿
    //在重置的时候会从文件读到这里面
    protected final PendingList batchList;
    private Iterator<MessageReference> iterator = null;
    protected boolean batchResetNeeded = false;
    //消息的个数
    protected int size;
    private final LinkedList<MessageId> pendingCachedIds = new LinkedList<>();
    private static int SYNC_ADD = 0;
    private static int ASYNC_ADD = 1;
    final MessageId[] lastCachedIds = new MessageId[2];
    protected boolean hadSpace = false;



    protected AbstractStoreCursor(Destination destination) {
        super((destination != null ? destination.isPrioritizedMessages():false));
        this.regionDestination=destination;
        if (this.prioritizedMessages) {
            this.batchList= new PrioritizedPendingList();
        } else {
            this.batchList = new OrderedPendingList();
        }
    }


    @Override
    public final synchronized void start() throws Exception{
        if (!isStarted()) {
            super.start();
            resetBatch();
            //设置消息的个数为统计对象的统计值
            resetSize();
            //为什么一定要size为0才可以呢 万一是重启了呢 难道是重启之后统计对象还没有重新统计吗
            setCacheEnabled(size==0&&useCache);
        }
    }

    //居然不是重置为0 这相当于校正啊 统计对象的消息个数肯定是对的啊
    protected void resetSize() {
        this.size = getStoreSize();
    }

    @Override
    public void rebase() {
        MessageId lastAdded = lastCachedIds[SYNC_ADD];
        if (lastAdded != null) {
            try {
                setBatch(lastAdded);
            } catch (Exception e) {
                LOG.error("{} - Failed to set batch on rebase", this, e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public final synchronized void stop() throws Exception {
        resetBatch();
        super.stop();
        gc();
    }


    @Override
    public final boolean recoverMessage(Message message) throws Exception {
        return recoverMessage(message,false);
    }

    //这个cached表示的是什么 看起来不像是表示是否缓存这个消息 更像是这个消息之前是否已经缓存了 处理topic的时候会传true
    //是不是可以认为cached表示的是这个消息是属于queue还是topic的呢
    //看着还是很像的 topic就是需要把消息存起来用于记录对应的订阅是否已经获得了这个消息 而queue不需要记录 只要被拿了就是消息完了
    //返回值有代表了什么呢 感觉并不是很关心的样子啊
    //看这个方法的名字 这段代码应该在恢复消息 返回值表示是否恢复成功
    //是不是只要消息从文件里读出来 就会调用这个方法呢 目前看来在两个地方调用过
    public synchronized boolean recoverMessage(Message message, boolean cached) throws Exception {
        boolean recovered = false;
        message.setRegionDestination(regionDestination);
        //表示消息之前还没有被用过
        //感觉这个逻辑要是判断不过的话 消息就不可能被恢复
        if (recordUniqueId(message.getMessageId())) {
            //是否缓存表示是否设置内存的使用
            if (!cached) {
                if( message.getMemoryUsage()==null ) {
                    message.setMemoryUsage(this.getSystemUsage().getMemoryUsage());
                }
            }
            //引用计算的类似功能吗
            message.incrementReferenceCount();
            //加到链表的后面
            batchList.addMessageLast(message);
            clearIterator(true);
            recovered = true;
        } else if (!cached) {
            // a duplicate from the store (!cached) - needs to be removed/acked - otherwise it will get re dispatched on restart
            if (duplicateFromStoreExcepted(message)) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("{} store replayed pending message due to concurrentStoreAndDispatchQueues {} seq: {}", this, message.getMessageId(), message.getMessageId().getFutureOrSequenceLong());
                }
            } else {
                LOG.warn("{} - cursor got duplicate from store {} seq: {}", this, message.getMessageId(), message.getMessageId().getFutureOrSequenceLong());
                duplicate(message);
            }
        } else {
            LOG.warn("{} - cursor got duplicate send {} seq: {}", this, message.getMessageId(), message.getMessageId().getFutureOrSequenceLong());
            if (gotToTheStore(message)) {
                duplicate(message);
            }
        }
        return recovered;
    }

    protected boolean duplicateFromStoreExcepted(Message message) {
        // expected for messages pending acks with kahadb.concurrentStoreAndDispatchQueues=true for
        // which this existing unused flag has been repurposed
        return message.isRecievedByDFBridge();
    }

    public static boolean gotToTheStore(Message message) throws Exception {
        if (message.isRecievedByDFBridge()) {
            // concurrent store and dispatch - wait to see if the message gets to the store to see
            // if the index suppressed it (original still present), or whether it was stored and needs to be removed
            Object possibleFuture = message.getMessageId().getFutureOrSequenceLong();
            if (possibleFuture instanceof Future) {
                ((Future) possibleFuture).get();
            }
            // need to access again after wait on future
            Object sequence = message.getMessageId().getFutureOrSequenceLong();
            return (sequence != null && sequence instanceof Long && Long.compare((Long) sequence, -1l) != 0);
        }
        return true;
    }

    // track for processing outside of store index lock so we can dlq
    //记录了所有的消息
    final LinkedList<Message> duplicatesFromStore = new LinkedList<Message>();
    private void duplicate(Message message) {
        duplicatesFromStore.add(message);
    }

    void dealWithDuplicates() {
        //看恢复消息的时候好像是没有进入可以直接读的集合的消息进入了这个集合 所以说进入这个集合的消息都是重复的
        for (Message message : duplicatesFromStore) {
            //目前看的是queue 订阅者返回的是空
            //这个方法会删除消息
            regionDestination.duplicateFromStore(message, getSubscription());
        }
        duplicatesFromStore.clear();
    }

    @Override
    public final synchronized void reset() {
        if (batchList.isEmpty()) {
            try {
                fillBatch();
            } catch (Exception e) {
                LOG.error("{} - Failed to fill batch", this, e);
                throw new RuntimeException(e);
            }
        }
        clearIterator(true);
        size();
    }


    @Override
    public synchronized void release() {
        clearIterator(false);
    }

    private synchronized void clearIterator(boolean ensureIterator) {
        boolean haveIterator = this.iterator != null;
        this.iterator=null;
        if(haveIterator&&ensureIterator) {
            ensureIterator();
        }
    }

    private synchronized void ensureIterator() {
        if(this.iterator==null) {
            this.iterator=this.batchList.iterator();
        }
    }


    public final void finished() {
    }


    @Override
    public final synchronized boolean hasNext() {
        if (batchList.isEmpty()) {
            try {
                fillBatch();
            } catch (Exception e) {
                LOG.error("{} - Failed to fill batch", this, e);
                throw new RuntimeException(e);
            }
        }
        ensureIterator();
        return this.iterator.hasNext();
    }


    @Override
    public final synchronized MessageReference next() {
        MessageReference result = null;
        if (!this.batchList.isEmpty()&&this.iterator.hasNext()) {
            result = this.iterator.next();
        }
        last = result;
        if (result != null) {
            result.incrementReferenceCount();
        }
        return result;
    }

    @Override
    public synchronized boolean tryAddMessageLast(MessageReference node, long wait) throws Exception {
        boolean disableCache = false;
        if (hasSpace()) {
            if (isCacheEnabled()) {
                if (recoverMessage(node.getMessage(),true)) {
                    trackLastCached(node);
                } else {
                    dealWithDuplicates();
                    return false;
                }
            }
        } else {
            disableCache = true;
        }

        if (disableCache && isCacheEnabled()) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("{} - disabling cache on add {} {}", this, node.getMessageId(), node.getMessageId().getFutureOrSequenceLong());
            }
            syncWithStore(node.getMessage());
            setCacheEnabled(false);
        }
        size++;
        return true;
    }

    @Override
    public synchronized boolean isCacheEnabled() {
        return super.isCacheEnabled() || enableCacheNow();
    }

    protected boolean enableCacheNow() {
        boolean result = false;
        if (canEnableCash()) {
            setCacheEnabled(true);
            result = true;
            if (LOG.isTraceEnabled()) {
                LOG.trace("{} enabling cache on empty store", this);
            }
        }
        return result;
    }

    protected boolean canEnableCash() {
        return useCache && size==0 && hasSpace() && isStarted();
    }

    private void syncWithStore(Message currentAdd) throws Exception {
        pruneLastCached();
        for (ListIterator<MessageId> it = pendingCachedIds.listIterator(pendingCachedIds.size()); it.hasPrevious(); ) {
            MessageId lastPending = it.previous();
            Object futureOrLong = lastPending.getFutureOrSequenceLong();
            if (futureOrLong instanceof Future) {
                Future future = (Future) futureOrLong;
                if (future.isCancelled()) {
                    continue;
                }
                try {
                    future.get(5, TimeUnit.SECONDS);
                    setLastCachedId(ASYNC_ADD, lastPending);
                } catch (CancellationException ok) {
                    continue;
                } catch (TimeoutException potentialDeadlock) {
                    LOG.debug("{} timed out waiting for async add", this, potentialDeadlock);
                } catch (Exception worstCaseWeReplay) {
                    LOG.debug("{} exception waiting for async add", this, worstCaseWeReplay);
                }
            } else {
                setLastCachedId(ASYNC_ADD, lastPending);
            }
            break;
        }

        MessageId candidate = lastCachedIds[ASYNC_ADD];
        if (candidate != null) {
            // ensure we don't skip current possibly sync add b/c we waited on the future
            if (!isAsync(currentAdd) && Long.compare(((Long) currentAdd.getMessageId().getFutureOrSequenceLong()), ((Long) lastCachedIds[ASYNC_ADD].getFutureOrSequenceLong())) < 0) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("no set batch from async:" + candidate.getFutureOrSequenceLong() + " >= than current: " + currentAdd.getMessageId().getFutureOrSequenceLong() + ", " + this);
                }
                candidate = null;
            }
        }
        if (candidate == null) {
            candidate = lastCachedIds[SYNC_ADD];
        }
        if (candidate != null) {
            setBatch(candidate);
        }
        // cleanup
        lastCachedIds[SYNC_ADD] = lastCachedIds[ASYNC_ADD] = null;
        pendingCachedIds.clear();
    }

    private void trackLastCached(MessageReference node) {
        if (isAsync(node.getMessage())) {
            pruneLastCached();
            pendingCachedIds.add(node.getMessageId());
        } else {
            setLastCachedId(SYNC_ADD, node.getMessageId());
        }
    }

    private static final boolean isAsync(Message message) {
        return message.isRecievedByDFBridge() || message.getMessageId().getFutureOrSequenceLong() instanceof Future;
    }

    private void pruneLastCached() {
        for (Iterator<MessageId> it = pendingCachedIds.iterator(); it.hasNext(); ) {
            MessageId candidate = it.next();
            final Object futureOrLong = candidate.getFutureOrSequenceLong();
            if (futureOrLong instanceof Future) {
                Future future = (Future) futureOrLong;
                if (future.isDone()) {
                    if (future.isCancelled()) {
                        it.remove();
                    } else {
                        // check for exception, we may be seeing old state
                        try {
                            future.get(0, TimeUnit.SECONDS);
                            // stale; if we get a result next prune will see Long
                        } catch (ExecutionException expected) {
                            it.remove();
                        } catch (Exception unexpected) {
                            LOG.debug("{} unexpected exception verifying exception state of future", this, unexpected);
                        }
                    }
                } else {
                    // we don't want to wait for work to complete
                    break;
                }
            } else {
                // complete
                setLastCachedId(ASYNC_ADD, candidate);

                // keep lock step with sync adds while order is preserved
                if (lastCachedIds[SYNC_ADD] != null) {
                    long next = 1 + (Long)lastCachedIds[SYNC_ADD].getFutureOrSequenceLong();
                    if (Long.compare((Long)futureOrLong, next) == 0) {
                        setLastCachedId(SYNC_ADD, candidate);
                    }
                }
                it.remove();
            }
        }
    }

    private void setLastCachedId(final int index, MessageId candidate) {
        MessageId lastCacheId = lastCachedIds[index];
        if (lastCacheId == null) {
            lastCachedIds[index] = candidate;
        } else {
            Object lastCacheFutureOrSequenceLong = lastCacheId.getFutureOrSequenceLong();
            Object candidateOrSequenceLong = candidate.getFutureOrSequenceLong();
            if (lastCacheFutureOrSequenceLong == null) { // possibly null for topics
                lastCachedIds[index] = candidate;
            } else if (candidateOrSequenceLong != null &&
                    Long.compare(((Long) candidateOrSequenceLong), ((Long) lastCacheFutureOrSequenceLong)) > 0) {
                lastCachedIds[index] = candidate;
            } else if (LOG.isTraceEnabled()) {
                LOG.trace("no set last cached[" + index + "] current:" + lastCacheFutureOrSequenceLong + " <= than candidate: " + candidateOrSequenceLong+ ", " + this);
            }
        }
    }

    protected void setBatch(MessageId messageId) throws Exception {
    }


    @Override
    public synchronized void addMessageFirst(MessageReference node) throws Exception {
        size++;
    }


    @Override
    public final synchronized void remove() {
        size--;
        if (iterator!=null) {
            iterator.remove();
        }
        if (last != null) {
            last.decrementReferenceCount();
        }
    }


    @Override
    public final synchronized void remove(MessageReference node) {
        if (batchList.remove(node) != null) {
            size--;
            setCacheEnabled(false);
        }
    }


    @Override
    public final synchronized void clear() {
        gc();
    }


    @Override
    public synchronized void gc() {
        for (MessageReference msg : batchList) {
            rollback(msg.getMessageId());
            msg.decrementReferenceCount();
        }
        batchList.clear();
        clearIterator(false);
        batchResetNeeded = true;
        setCacheEnabled(false);
    }

    @Override
    protected final synchronized void fillBatch() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("{} fillBatch", this);
        }
        if (batchResetNeeded) {
            //重新读取一下size
            resetSize();
            //这个是设置一次读取多少吗
            setMaxBatchSize(Math.min(regionDestination.getMaxPageSize(), size));
            resetBatch();
            this.batchResetNeeded = false;
        }
        if (this.batchList.isEmpty() && this.size >0) {
            try {
                doFillBatch();
            } catch (Exception e) {
                LOG.error("{} - Failed to fill batch", this, e);
                throw new RuntimeException(e);
            }
        }
    }


    @Override
    public final synchronized boolean isEmpty() {
        // negative means more messages added to store through queue.send since last reset
        return size == 0;
    }


    @Override
    public final synchronized boolean hasMessagesBufferedToDeliver() {
        return !batchList.isEmpty();
    }


    @Override
    public final synchronized int size() {
        if (size < 0) {
            this.size = getStoreSize();
        }
        return size;
    }

    @Override
    public final synchronized long messageSize() {
        return getStoreMessageSize();
    }

    @Override
    public String toString() {
        return super.toString() + ":" + regionDestination.getActiveMQDestination().getPhysicalName() + ",batchResetNeeded=" + batchResetNeeded
                    + ",size=" + this.size + ",cacheEnabled=" + cacheEnabled
                    + ",maxBatchSize:" + maxBatchSize + ",hasSpace:" + hasSpace() + ",pendingCachedIds.size:" + pendingCachedIds.size()
                    + ",lastSyncCachedId:" + lastCachedIds[SYNC_ADD] + ",lastSyncCachedId-seq:" + (lastCachedIds[SYNC_ADD] != null ? lastCachedIds[SYNC_ADD].getFutureOrSequenceLong() : "null")
                    + ",lastAsyncCachedId:" + lastCachedIds[ASYNC_ADD] + ",lastAsyncCachedId-seq:" + (lastCachedIds[ASYNC_ADD] != null ? lastCachedIds[ASYNC_ADD].getFutureOrSequenceLong() : "null");
    }

    protected abstract void doFillBatch() throws Exception;

    protected abstract void resetBatch();

    protected abstract int getStoreSize();

    protected abstract long getStoreMessageSize();

    protected abstract boolean isStoreEmpty();

    public Subscription getSubscription() {
        return null;
    }
}
