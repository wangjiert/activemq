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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store based Cursor for Queues
 */
public class StoreQueueCursor extends AbstractPendingMessageCursor {

    private static final Logger LOG = LoggerFactory.getLogger(StoreQueueCursor.class);
    private final Broker broker;
    //说明这个地址里面总共有多少条消息
    //但是当地址没有启动时，消息也进去了，这个变量的值却没变
    private int pendingCount;
    private final Queue queue;
    //感觉这个是放在内存中的消息缓冲
    //下面初始化明明是放在文件中的 为什么要加non在变量前面 看来下面的应该就是在最终存放消息的地方
    private PendingMessageCursor nonPersistent;
    //这不是预取吗 难道说这个是放在持久化中的消息缓冲
    //一个大胆的猜想 persistent是代表的磁盘上的消息  nonPersistent代表的是从磁盘上读取到内存中的消息
    //至于为什么nonPersistent里面还分持久化的和非持久化的 应该是内存里过多之后为了节约内存把一部分消息写到了文件中
    //至于为什么这两个持久化与非持久化之间能够做到不重叠 暂时还没想到
    private final QueueStorePrefetch persistent;
    //这个是综合上面两个的吗
    //取消息的时候首先是从这里取的
    //这个东西和交换的被赋值为persistent和nonPersistent
    private PendingMessageCursor currentCursor;

    /**
     * Construct
     * @param broker
     * @param queue
     */
    public StoreQueueCursor(Broker broker,Queue queue) {
        super((queue != null ? queue.isPrioritizedMessages():false));
        this.broker=broker;
        //这个缓存属于的地址
        this.queue = queue;
        this.persistent = new QueueStorePrefetch(queue, broker);
        currentCursor = persistent;
    }

    @Override
    public synchronized void start() throws Exception {
        started = true;
        super.start();
        if (nonPersistent == null) {
            if (broker.getBrokerService().isPersistent()) {
                nonPersistent = new FilePendingMessageCursor(broker,queue.getName(),this.prioritizedMessages);
            }else {
                nonPersistent = new VMPendingMessageCursor(this.prioritizedMessages);
            }
            nonPersistent.setMaxBatchSize(getMaxBatchSize());
            nonPersistent.setSystemUsage(systemUsage);
            nonPersistent.setEnableAudit(isEnableAudit());
            nonPersistent.setMaxAuditDepth(getMaxAuditDepth());
            nonPersistent.setMaxProducersToAudit(getMaxProducersToAudit());
        }
        //两个存储用用一个审计对象
        nonPersistent.setMessageAudit(getMessageAudit());
        //就是添加一个监听器吗
        nonPersistent.start();
        persistent.setMessageAudit(getMessageAudit());
        persistent.start();
        pendingCount = persistent.size() + nonPersistent.size();
    }

    @Override
    public synchronized void stop() throws Exception {
        started = false;
        if (nonPersistent != null) {
          nonPersistent.destroy();
        }
        persistent.stop();
        persistent.gc();
        super.stop();
        pendingCount = 0;
    }

    @Override
    //返回值应该是表示是否添加成功
    //根据消息是否是持久化的好像被放到不同的地方
    //取消息的时候会从两个地方取 但是这里放进去的时候又是将持久化消息与非持久化消息分开放
    //感觉很符合逻辑 这样就保证了两个地方的消息不会重复
    //持久化到底是指一个消息本身 还是说指一个地址呢
    //现在来看 好像持久化是生产者设置的
    //也就是说一个地址里面既有持久化消息也有飞持久化消息 那么问题来了 非持久化里面的消息也会放到文件里面是为什么呢  难道是怕消息过多导致内存溢出吗
    public synchronized boolean tryAddMessageLast(MessageReference node, long maxWait) throws Exception {
        boolean result = true;
        if (node != null) {
            Message msg = node.getMessage();
            //地址没有启动的时候只有非持久消息会丢弃
            if (started) {
                pendingCount++;
                if (!msg.isPersistent()) {
                    result = nonPersistent.tryAddMessageLast(node, maxWait);
                }
            }
            if (msg.isPersistent()) {
                //感觉啥都没干 的确没什么存的 反正持久化的消息已经在存过一次了 只有非持久化的消息才有需要在文件或则内存中存放
                result = persistent.addMessageLast(node);
            }
        }
        return result;
    }

    @Override
    public synchronized void addMessageFirst(MessageReference node) throws Exception {
        if (node != null) {
            Message msg = node.getMessage();
            if (started) {
                pendingCount++;
                if (!msg.isPersistent()) {
                    nonPersistent.addMessageFirst(node);
                }
            }
            if (msg.isPersistent()) {
                persistent.addMessageFirst(node);
            }
        }
    }

    @Override
    public synchronized void clear() {
        pendingCount = 0;
    }

    @Override
    public synchronized boolean hasNext() {
        try {
            //就是给currentCursor赋值
            getNextCursor();
        } catch (Exception e) {
            LOG.error("Failed to get current cursor ", e);
            throw new RuntimeException(e);
       }
       return currentCursor != null ? currentCursor.hasNext() : false;
    }

    @Override
    public synchronized MessageReference next() {
        MessageReference result = currentCursor != null ? currentCursor.next() : null;
        return result;
    }

    @Override
    public synchronized void remove() {
        if (currentCursor != null) {
            currentCursor.remove();
        }
        pendingCount--;
    }

    @Override
    public synchronized void remove(MessageReference node) {
        if (!node.isPersistent()) {
            nonPersistent.remove(node);
        } else {
            persistent.remove(node);
        }
        pendingCount--;
    }

    @Override
    public synchronized void reset() {
        nonPersistent.reset();
        persistent.reset();
        pendingCount = persistent.size() + nonPersistent.size();
    }

    @Override
    public void release() {
        nonPersistent.release();
        persistent.release();
    }


    @Override
    public synchronized int size() {
        if (pendingCount < 0) {
            pendingCount = persistent.size() + nonPersistent.size();
        }
        return pendingCount;
    }

    @Override
    public synchronized long messageSize() {
        return persistent.messageSize() + nonPersistent.messageSize();
    }

    @Override
    public synchronized boolean isEmpty() {
        // if negative, more messages arrived in store since last reset so non empty
        return pendingCount == 0;
    }

    /**
     * Informs the Broker if the subscription needs to intervention to recover
     * it's state e.g. DurableTopicSubscriber may do
     *
     * @see org.apache.activemq.broker.region.cursors.PendingMessageCursor
     * @return true if recovery required
     */
    @Override
    public boolean isRecoveryRequired() {
        return false;
    }

    /**
     * @return the nonPersistent Cursor
     */
    public PendingMessageCursor getNonPersistent() {
        return this.nonPersistent;
    }

    /**
     * @param nonPersistent cursor to set
     */
    public void setNonPersistent(PendingMessageCursor nonPersistent) {
        this.nonPersistent = nonPersistent;
    }

    /**
     * @return the persistent Cursor
     */
    public PendingMessageCursor getPersistent() { return  this.persistent; }

    @Override
    //这个方法只是设置一次取多少消息吧
    public void setMaxBatchSize(int maxBatchSize) {
        persistent.setMaxBatchSize(maxBatchSize);
        if (nonPersistent != null) {
            nonPersistent.setMaxBatchSize(maxBatchSize);
        }
        super.setMaxBatchSize(maxBatchSize);
    }


    @Override
    public void setMaxProducersToAudit(int maxProducersToAudit) {
        super.setMaxProducersToAudit(maxProducersToAudit);
        if (persistent != null) {
            persistent.setMaxProducersToAudit(maxProducersToAudit);
        }
        if (nonPersistent != null) {
            nonPersistent.setMaxProducersToAudit(maxProducersToAudit);
        }
    }

    @Override
    public void setMaxAuditDepth(int maxAuditDepth) {
        super.setMaxAuditDepth(maxAuditDepth);
        if (persistent != null) {
            persistent.setMaxAuditDepth(maxAuditDepth);
        }
        if (nonPersistent != null) {
            nonPersistent.setMaxAuditDepth(maxAuditDepth);
        }
    }

    @Override
    public void setEnableAudit(boolean enableAudit) {
        super.setEnableAudit(enableAudit);
        if (persistent != null) {
            persistent.setEnableAudit(enableAudit);
        }
        if (nonPersistent != null) {
            nonPersistent.setEnableAudit(enableAudit);
        }
    }

    @Override
    public void rollback(MessageId id) {
        nonPersistent.rollback(id);
        persistent.rollback(id);
    }

    @Override
    public void setUseCache(boolean useCache) {
        super.setUseCache(useCache);
        if (persistent != null) {
            persistent.setUseCache(useCache);
        }
        if (nonPersistent != null) {
            nonPersistent.setUseCache(useCache);
        }
    }

    @Override
    public void setMemoryUsageHighWaterMark(int memoryUsageHighWaterMark) {
        super.setMemoryUsageHighWaterMark(memoryUsageHighWaterMark);
        if (persistent != null) {
            persistent.setMemoryUsageHighWaterMark(memoryUsageHighWaterMark);
        }
        if (nonPersistent != null) {
            nonPersistent.setMemoryUsageHighWaterMark(memoryUsageHighWaterMark);
        }
    }



    @Override
    public synchronized void gc() {
        if (persistent != null) {
            persistent.gc();
        }
        if (nonPersistent != null) {
            nonPersistent.gc();
        }
        pendingCount = persistent.size() + nonPersistent.size();
    }

    @Override
    public void setSystemUsage(SystemUsage usageManager) {
        super.setSystemUsage(usageManager);
        if (persistent != null) {
            persistent.setSystemUsage(usageManager);
        }
        if (nonPersistent != null) {
            nonPersistent.setSystemUsage(usageManager);
        }
    }

    protected synchronized PendingMessageCursor getNextCursor() throws Exception {
        if (currentCursor == null || !currentCursor.hasMessagesBufferedToDeliver()) {
            currentCursor = currentCursor == persistent ? nonPersistent : persistent;
            // sanity check
            if (currentCursor.isEmpty()) {
                currentCursor = currentCursor == persistent ? nonPersistent : persistent;
            }
        }
        return currentCursor;
    }

    @Override
    //只有三个同时是允许缓存的 才会返回true
    public boolean isCacheEnabled() {
        boolean cacheEnabled = isUseCache();
        if (cacheEnabled) {
            if (persistent != null) {
                cacheEnabled &= persistent.isCacheEnabled();
            }
            if (nonPersistent != null) {
                cacheEnabled &= nonPersistent.isCacheEnabled();
            }
            setCacheEnabled(cacheEnabled);
        }
        return cacheEnabled;
    }

    @Override
    public void rebase() {
        persistent.rebase();
        reset();
    }

}
