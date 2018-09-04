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
package org.apache.activemq.broker.region;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.jms.InvalidClientIDException;
import javax.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.EmptyBroker;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.DeadLetterStrategy;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.state.ConnectionState;
import org.apache.activemq.store.PListStore;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.TransmitCallback;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.BrokerSupport;
import org.apache.activemq.util.IdGenerator;
import org.apache.activemq.util.InetAddressUtil;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes Broker operations to the correct messaging regions for processing.
 */
public class RegionBroker extends EmptyBroker {
    public static final String ORIGINAL_EXPIRATION = "originalExpiration";
    private static final Logger LOG = LoggerFactory.getLogger(RegionBroker.class);
    private static final IdGenerator BROKER_ID_GENERATOR = new IdGenerator();

    protected final DestinationStatistics destinationStatistics = new DestinationStatistics();
    protected DestinationFactory destinationFactory;
    protected final Map<ConnectionId, ConnectionState> connectionStates = Collections.synchronizedMap(new HashMap<ConnectionId, ConnectionState>());

    private final Region queueRegion;
    private final Region topicRegion;
    private final Region tempQueueRegion;
    private final Region tempTopicRegion;
    protected final BrokerService brokerService;
    private boolean started;
    private boolean keepDurableSubsActive;

    //这个里面连接可能有点多 有可能没有删除被覆盖的连接
    private final CopyOnWriteArrayList<Connection> connections = new CopyOnWriteArrayList<Connection>();
    private final Map<ActiveMQDestination, ActiveMQDestination> destinationGate = new HashMap<ActiveMQDestination, ActiveMQDestination>();
    //分别是什么类型 应该前面的是标准的jms地址 后面的应该是broker里面的地址
    private final Map<ActiveMQDestination, Destination> destinations = new ConcurrentHashMap<ActiveMQDestination, Destination>();
    private final Map<BrokerId, BrokerInfo> brokerInfos = new HashMap<BrokerId, BrokerInfo>();

    private final LongSequenceGenerator sequenceGenerator = new LongSequenceGenerator();
    private BrokerId brokerId;
    private String brokerName;
    private final Map<String, ConnectionContext> clientIdSet = new HashMap<String, ConnectionContext>();
    private final DestinationInterceptor destinationInterceptor;
    private ConnectionContext adminConnectionContext;
    private final Scheduler scheduler;
    private final ThreadPoolExecutor executor;
    private boolean allowTempAutoCreationOnSend;

    private final ReentrantReadWriteLock inactiveDestinationsPurgeLock = new ReentrantReadWriteLock();
    private final TaskRunnerFactory taskRunnerFactory;
    private final AtomicBoolean purgeInactiveDestinationsTaskInProgress = new AtomicBoolean(false);
    private final Runnable purgeInactiveDestinationsTask = new Runnable() {
        @Override
        public void run() {
            if (purgeInactiveDestinationsTaskInProgress.compareAndSet(false, true)) {
                taskRunnerFactory.execute(purgeInactiveDestinationsWork);
            }
        }
    };
    private final Runnable purgeInactiveDestinationsWork = new Runnable() {
        @Override
        public void run() {
            try {
                purgeInactiveDestinations();
            } catch (Throwable ignored) {
                LOG.error("Unexpected exception on purgeInactiveDestinations {}", this, ignored);
            } finally {
                purgeInactiveDestinationsTaskInProgress.set(false);
            }
        }
    };

    public RegionBroker(BrokerService brokerService, TaskRunnerFactory taskRunnerFactory, SystemUsage memoryManager, DestinationFactory destinationFactory,
        DestinationInterceptor destinationInterceptor, Scheduler scheduler, ThreadPoolExecutor executor) throws IOException {
        this.brokerService = brokerService;
        this.executor = executor;
        this.scheduler = scheduler;
        if (destinationFactory == null) {
            throw new IllegalArgumentException("null destinationFactory");
        }
        this.sequenceGenerator.setLastSequenceId(destinationFactory.getLastMessageBrokerSequenceId());
        this.destinationFactory = destinationFactory;
        queueRegion = createQueueRegion(memoryManager, taskRunnerFactory, destinationFactory);
        topicRegion = createTopicRegion(memoryManager, taskRunnerFactory, destinationFactory);
        this.destinationInterceptor = destinationInterceptor;
        tempQueueRegion = createTempQueueRegion(memoryManager, taskRunnerFactory, destinationFactory);
        tempTopicRegion = createTempTopicRegion(memoryManager, taskRunnerFactory, destinationFactory);
        this.taskRunnerFactory = taskRunnerFactory;
    }

    @Override
    public Map<ActiveMQDestination, Destination> getDestinationMap() {
        Map<ActiveMQDestination, Destination> answer = new HashMap<ActiveMQDestination, Destination>(getQueueRegion().getDestinationMap());
        answer.putAll(getTopicRegion().getDestinationMap());
        return answer;
    }

    @Override
    public Map<ActiveMQDestination, Destination> getDestinationMap(ActiveMQDestination destination) {
        try {
            return getRegion(destination).getDestinationMap();
        } catch (JMSException jmse) {
            return Collections.emptyMap();
        }
    }

    @Override
    public Set<Destination> getDestinations(ActiveMQDestination destination) {
        try {
            return getRegion(destination).getDestinations(destination);
        } catch (JMSException jmse) {
            return Collections.emptySet();
        }
    }

    public Region getQueueRegion() {
        return queueRegion;
    }

    public Region getTempQueueRegion() {
        return tempQueueRegion;
    }

    public Region getTempTopicRegion() {
        return tempTopicRegion;
    }

    public Region getTopicRegion() {
        return topicRegion;
    }

    protected Region createTempTopicRegion(SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        return new TempTopicRegion(this, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    protected Region createTempQueueRegion(SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        return new TempQueueRegion(this, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    protected Region createTopicRegion(SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        return new TopicRegion(this, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    protected Region createQueueRegion(SystemUsage memoryManager, TaskRunnerFactory taskRunnerFactory, DestinationFactory destinationFactory) {
        return new QueueRegion(this, destinationStatistics, memoryManager, taskRunnerFactory, destinationFactory);
    }

    @Override
    public void start() throws Exception {
        started = true;
        queueRegion.start();
        topicRegion.start();
        tempQueueRegion.start();
        tempTopicRegion.start();
        int period = this.brokerService.getSchedulePeriodForDestinationPurge();
        if (period > 0) {
            this.scheduler.executePeriodically(purgeInactiveDestinationsTask, period);
        }
    }

    @Override
    public void stop() throws Exception {
        started = false;
        this.scheduler.cancel(purgeInactiveDestinationsTask);
        ServiceStopper ss = new ServiceStopper();
        doStop(ss);
        ss.throwFirstException();
        // clear the state
        clientIdSet.clear();
        connections.clear();
        destinations.clear();
        brokerInfos.clear();
    }

    public PolicyMap getDestinationPolicy() {
        return brokerService != null ? brokerService.getDestinationPolicy() : null;
    }

    public ConnectionContext getConnectionContext(String clientId) {
        return clientIdSet.get(clientId);
    }

    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        String clientId = info.getClientId();
        if (clientId == null) {
            throw new InvalidClientIDException("No clientID specified for connection request");
        }

        ConnectionContext oldContext = null;

        synchronized (clientIdSet) {
            //客户id和连接上下文的集合
            oldContext = clientIdSet.get(clientId);
            if (oldContext != null) {
                if (context.isAllowLinkStealing()) {
                    clientIdSet.put(clientId, context);
                } else {
                    throw new InvalidClientIDException("Broker: " + getBrokerName() + " - Client: " + clientId + " already connected from "
                        + oldContext.getConnection().getRemoteAddress());
                }
            } else {
                clientIdSet.put(clientId, context);
            }
        }

        if (oldContext != null) {
            if (oldContext.getConnection() != null) {
                Connection connection = oldContext.getConnection();
                LOG.warn("Stealing link for clientId {} From Connection {}", clientId, oldContext.getConnection());
                if (connection instanceof TransportConnection) {
                    TransportConnection transportConnection = (TransportConnection) connection;
                    transportConnection.stopAsync(new IOException("Stealing link for clientId " + clientId + " From Connection " + oldContext.getConnection().getConnectionId()));
                } else {
                    connection.stop();
                }
            } else {
                LOG.error("No Connection found for {}", oldContext);
            }
        }

        //加进去了新的 也没有把旧的删除掉呢
        //还是说在上面的stop里面会有删除呢
        connections.add(context.getConnection());
    }

    @Override
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        String clientId = info.getClientId();
        if (clientId == null) {
            throw new InvalidClientIDException("No clientID specified for connection disconnect request");
        }
        synchronized (clientIdSet) {
            ConnectionContext oldValue = clientIdSet.get(clientId);
            // we may be removing the duplicate connection, not the first connection to be created
            // so lets check that their connection IDs are the same
            if (oldValue == context) {
                if (isEqual(oldValue.getConnectionId(), info.getConnectionId())) {
                    clientIdSet.remove(clientId);
                }
            }
        }
        connections.remove(context.getConnection());
    }

    protected boolean isEqual(ConnectionId connectionId, ConnectionId connectionId2) {
        return connectionId == connectionId2 || (connectionId != null && connectionId.equals(connectionId2));
    }

    @Override
    public Connection[] getClients() throws Exception {
        ArrayList<Connection> l = new ArrayList<Connection>(connections);
        Connection rc[] = new Connection[l.size()];
        l.toArray(rc);
        return rc;
    }

    @Override
    //根据jms地址创建一个broker内部地址
    //会不会是由于客户端的多线程效果导致这个方法对于同一个地址进入多次
    public Destination addDestination(ConnectionContext context, ActiveMQDestination destination, boolean createIfTemp) throws Exception {

        Destination answer;

        //如果已经添加过了 就直接返回了
        //既然不会有重复的地址进来 那么订阅怎么会扫描到和这个地址相同的订阅呢
        //这个缓存也是broker中的缓存呀 难道每个连接都有自己的broker对象吗
        //难道是jms的地址和broker内的地址是多对一的关系吗
        answer = destinations.get(destination);
        if (answer != null) {
            return answer;
        }

        //又是一段废代码 鉴定完毕
        synchronized (destinationGate) {
            answer = destinations.get(destination);
            if (answer != null) {
                return answer;
            }

            //首先什么时候往里面加的
            //既然已经知道了集合里面已经加了 那么不停的睡眠是在等谁会从集合里面拿走
            //todo 找到这个集合真正处理的地方
            //在下面的模块中会从集合里面删除这个地址 但是还是没看到是在哪里加进去的呀
            //奇了怪了 搜整个项目也没看到是在哪加进去的
            if (destinationGate.get(destination) != null) {
                // Guard against spurious wakeup.
                while (destinationGate.containsKey(destination)) {
                    destinationGate.wait();
                }
                answer = destinations.get(destination);
                if (answer != null) {
                    return answer;
                } else {
                    // In case of intermediate remove or add failure
                    //这么会玩吗  只有这里往里面加东西了 但是进入这里的条件是之前必须已经加过看
                    destinationGate.put(destination, destination);
                }
            }
        }

        //直接从这里看
        try {
            boolean create = true;
            if (destination.isTemporary()) {
                create = createIfTemp;
            }
            //以queue region为例
            //等同于一个单例  地址在这里仅仅是提供了一个类型 并不是说每一个地址都去创建一个broker内部的地址
            answer = getRegion(destination).addDestination(context, destination, create);
            //这些所谓的缓存是怎么控制缓存数量的呢 现在看到的都是不停的加
            //还是说这些东西很少并且不怎么占空间 所以可以全部缓存下来
            destinations.put(destination, answer);
        } finally {
            //没有用的代码
            synchronized (destinationGate) {
                destinationGate.remove(destination);
                destinationGate.notifyAll();
            }
        }

        return answer;
    }

    @Override
    public void removeDestination(ConnectionContext context, ActiveMQDestination destination, long timeout) throws Exception {
        if (destinations.containsKey(destination)) {
            getRegion(destination).removeDestination(context, destination, timeout);
            destinations.remove(destination);
        }
    }

    @Override
    public void addDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        addDestination(context, info.getDestination(), true);

    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        removeDestination(context, info.getDestination(), info.getTimeout());
    }

    @Override
    public ActiveMQDestination[] getDestinations() throws Exception {
        ArrayList<ActiveMQDestination> l;

        l = new ArrayList<ActiveMQDestination>(getDestinationMap().keySet());

        ActiveMQDestination rc[] = new ActiveMQDestination[l.size()];
        l.toArray(rc);
        return rc;
    }

    @Override
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        ActiveMQDestination destination = info.getDestination();
        if (destination != null) {
            inactiveDestinationsPurgeLock.readLock().lock();
            try {
                // This seems to cause the destination to be added but without
                // advisories firing...
                //已经在一个broker里面了 为什么还在通过连接上下文取broker 看来broker对象是不唯一的
                //搞笑的吧 添加地址的时候已经调过这个方法了 现在添加消费者又来一次 是几个意思呢
                context.getBroker().addDestination(context, destination, isAllowTempAutoCreationOnSend());
                getRegion(destination).addProducer(context, info);
            } finally {
                inactiveDestinationsPurgeLock.readLock().unlock();
            }
        }
    }

    @Override
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        ActiveMQDestination destination = info.getDestination();
        if (destination != null) {
            inactiveDestinationsPurgeLock.readLock().lock();
            try {
                getRegion(destination).removeProducer(context, info);
            } finally {
                inactiveDestinationsPurgeLock.readLock().unlock();
            }
        }
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        ActiveMQDestination destination = info.getDestination();
        if (destinationInterceptor != null) {
            destinationInterceptor.create(this, context, destination);
        }
        inactiveDestinationsPurgeLock.readLock().lock();
        try {
            //之前添加地址的时候已经加入了jms地址月broker内部地址的一一对应
            return getRegion(destination).addConsumer(context, info);
        } finally {
            inactiveDestinationsPurgeLock.readLock().unlock();
        }
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        ActiveMQDestination destination = info.getDestination();
        inactiveDestinationsPurgeLock.readLock().lock();
        try {
            getRegion(destination).removeConsumer(context, info);
        } finally {
            inactiveDestinationsPurgeLock.readLock().unlock();
        }
    }

    @Override
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        inactiveDestinationsPurgeLock.readLock().lock();
        try {
            topicRegion.removeSubscription(context, info);
        } finally {
            inactiveDestinationsPurgeLock.readLock().unlock();

        }
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message message) throws Exception {
        ActiveMQDestination destination = message.getDestination();
        message.setBrokerInTime(System.currentTimeMillis());
        if (producerExchange.isMutable() || producerExchange.getRegion() == null
            || (producerExchange.getRegionDestination() != null && producerExchange.getRegionDestination().isDisposed())) {
            // ensure the destination is registered with the RegionBroker
            producerExchange.getConnectionContext().getBroker()
                .addDestination(producerExchange.getConnectionContext(), destination, isAllowTempAutoCreationOnSend());
            //这个region是单例的
            producerExchange.setRegion(getRegion(destination));
            //这个又是干什么呢
            producerExchange.setRegionDestination(null);
        }

        producerExchange.getRegion().send(producerExchange, message);

        // clean up so these references aren't kept (possible leak) in the producer exchange
        // especially since temps are transitory
        if (producerExchange.isMutable()) {
            producerExchange.setRegionDestination(null);
            producerExchange.setRegion(null);
        }
    }

    @Override
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        if (consumerExchange.isWildcard() || consumerExchange.getRegion() == null) {
            ActiveMQDestination destination = ack.getDestination();
            consumerExchange.setRegion(getRegion(destination));
        }
        //所有同类型的地址对应同一个region
        consumerExchange.getRegion().acknowledge(consumerExchange, ack);
    }

    public Region getRegion(ActiveMQDestination destination) throws JMSException {
        switch (destination.getDestinationType()) {
            case ActiveMQDestination.QUEUE_TYPE:
                return queueRegion;
            case ActiveMQDestination.TOPIC_TYPE:
                return topicRegion;
            case ActiveMQDestination.TEMP_QUEUE_TYPE:
                return tempQueueRegion;
            case ActiveMQDestination.TEMP_TOPIC_TYPE:
                return tempTopicRegion;
            default:
                throw createUnknownDestinationTypeException(destination);
        }
    }

    @Override
    public Response messagePull(ConnectionContext context, MessagePull pull) throws Exception {
        ActiveMQDestination destination = pull.getDestination();
        //region就是queue region,集成了 abstract region
        return getRegion(destination).messagePull(context, pull);
    }

    @Override
    public TransactionId[] getPreparedTransactions(ConnectionContext context) throws Exception {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    @Override
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    @Override
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    @Override
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    @Override
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    @Override
    public void forgetTransaction(ConnectionContext context, TransactionId transactionId) throws Exception {
        throw new IllegalAccessException("Transaction operation not implemented by this broker.");
    }

    @Override
    public void gc() {
        queueRegion.gc();
        topicRegion.gc();
    }

    @Override
    public BrokerId getBrokerId() {
        if (brokerId == null) {
            brokerId = new BrokerId(BROKER_ID_GENERATOR.generateId());
        }
        return brokerId;
    }

    public void setBrokerId(BrokerId brokerId) {
        this.brokerId = brokerId;
    }

    @Override
    public String getBrokerName() {
        if (brokerName == null) {
            try {
                brokerName = InetAddressUtil.getLocalHostName().toLowerCase(Locale.ENGLISH);
            } catch (Exception e) {
                brokerName = "localhost";
            }
        }
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public DestinationStatistics getDestinationStatistics() {
        return destinationStatistics;
    }

    protected JMSException createUnknownDestinationTypeException(ActiveMQDestination destination) {
        return new JMSException("Unknown destination type: " + destination.getDestinationType());
    }

    @Override
    public synchronized void addBroker(Connection connection, BrokerInfo info) {
        BrokerInfo existing = brokerInfos.get(info.getBrokerId());
        if (existing == null) {
            existing = info.copy();
            existing.setPeerBrokerInfos(null);
            brokerInfos.put(info.getBrokerId(), existing);
        }
        existing.incrementRefCount();
        LOG.debug("{} addBroker: {} brokerInfo size: {}", new Object[]{ getBrokerName(), info.getBrokerName(), brokerInfos.size() });
        addBrokerInClusterUpdate(info);
    }

    @Override
    public synchronized void removeBroker(Connection connection, BrokerInfo info) {
        if (info != null) {
            BrokerInfo existing = brokerInfos.get(info.getBrokerId());
            if (existing != null && existing.decrementRefCount() == 0) {
                brokerInfos.remove(info.getBrokerId());
            }
            LOG.debug("{} removeBroker: {} brokerInfo size: {}", new Object[]{ getBrokerName(), info.getBrokerName(), brokerInfos.size()});
            // When stopping don't send cluster updates since we are the one's tearing down
            // our own bridges.
            if (!brokerService.isStopping()) {
                removeBrokerInClusterUpdate(info);
            }
        }
    }

    @Override
    public synchronized BrokerInfo[] getPeerBrokerInfos() {
        BrokerInfo[] result = new BrokerInfo[brokerInfos.size()];
        result = brokerInfos.values().toArray(result);
        return result;
    }

    @Override
    public void preProcessDispatch(final MessageDispatch messageDispatch) {
        final Message message = messageDispatch.getMessage();
        if (message != null) {
            long endTime = System.currentTimeMillis();
            message.setBrokerOutTime(endTime);
            if (getBrokerService().isEnableStatistics()) {
                long totalTime = endTime - message.getBrokerInTime();
                ((Destination) message.getRegionDestination()).getDestinationStatistics().getProcessTime().addTime(totalTime);
            }
            if (((BaseDestination) message.getRegionDestination()).isPersistJMSRedelivered() && !message.isRedelivered()) {
                final int originalValue = message.getRedeliveryCounter();
                message.incrementRedeliveryCounter();
                try {
                    if (message.isPersistent()) {
                        ((BaseDestination) message.getRegionDestination()).getMessageStore().updateMessage(message);
                    }
                    messageDispatch.setTransmitCallback(new TransmitCallback() {
                        // dispatch is considered a delivery, so update sub state post dispatch otherwise
                        // on a disconnect/reconnect cached messages will not reflect initial delivery attempt
                        final TransmitCallback delegate = messageDispatch.getTransmitCallback();
                        @Override
                        public void onSuccess() {
                            message.incrementRedeliveryCounter();
                            if (delegate != null) {
                                delegate.onSuccess();
                            }
                        }

                        @Override
                        public void onFailure() {
                            if (delegate != null) {
                                delegate.onFailure();
                            }
                        }
                    });
                } catch (IOException error) {
                    RuntimeException runtimeException = new RuntimeException("Failed to persist JMSRedeliveryFlag on " + message.getMessageId() + " in " + message.getDestination(), error);
                    LOG.warn(runtimeException.getLocalizedMessage(), runtimeException);
                    throw runtimeException;
                } finally {
                    message.setRedeliveryCounter(originalValue);
                }
            }
        }
    }

    @Override
    public void postProcessDispatch(MessageDispatch messageDispatch) {
    }

    @Override
    public void processDispatchNotification(MessageDispatchNotification messageDispatchNotification) throws Exception {
        ActiveMQDestination destination = messageDispatchNotification.getDestination();
        getRegion(destination).processDispatchNotification(messageDispatchNotification);
    }

    @Override
    public boolean isStopped() {
        return !started;
    }

    @Override
    public Set<ActiveMQDestination> getDurableDestinations() {
        return destinationFactory.getDestinations();
    }

    protected void doStop(ServiceStopper ss) {
        ss.stop(queueRegion);
        ss.stop(topicRegion);
        ss.stop(tempQueueRegion);
        ss.stop(tempTopicRegion);
    }

    public boolean isKeepDurableSubsActive() {
        return keepDurableSubsActive;
    }

    public void setKeepDurableSubsActive(boolean keepDurableSubsActive) {
        this.keepDurableSubsActive = keepDurableSubsActive;
        ((TopicRegion) topicRegion).setKeepDurableSubsActive(keepDurableSubsActive);
    }

    public DestinationInterceptor getDestinationInterceptor() {
        return destinationInterceptor;
    }

    @Override
    public ConnectionContext getAdminConnectionContext() {
        return adminConnectionContext;
    }

    @Override
    public void setAdminConnectionContext(ConnectionContext adminConnectionContext) {
        this.adminConnectionContext = adminConnectionContext;
    }

    public Map<ConnectionId, ConnectionState> getConnectionStates() {
        return connectionStates;
    }

    @Override
    public PListStore getTempDataStore() {
        return brokerService.getTempDataStore();
    }

    @Override
    public URI getVmConnectorURI() {
        return brokerService.getVmConnectorURI();
    }

    @Override
    public void brokerServiceStarted() {
    }

    @Override
    public BrokerService getBrokerService() {
        return brokerService;
    }

    @Override
    public boolean isExpired(MessageReference messageReference) {
        return messageReference.canProcessAsExpired();
    }

    private boolean stampAsExpired(Message message) throws IOException {
        boolean stamped = false;
        if (message.getProperty(ORIGINAL_EXPIRATION) == null) {
            long expiration = message.getExpiration();
            message.setProperty(ORIGINAL_EXPIRATION, new Long(expiration));
            stamped = true;
        }
        return stamped;
    }

    @Override
    public void messageExpired(ConnectionContext context, MessageReference node, Subscription subscription) {
        LOG.debug("Message expired {}", node);
        getRoot().sendToDeadLetterQueue(context, node, subscription, new Throwable("Message Expired. Expiration:" + node.getExpiration()));
    }

    @Override
    public boolean sendToDeadLetterQueue(ConnectionContext context, MessageReference node, Subscription subscription, Throwable poisonCause) {
        try {
            if (node != null) {
                Message message = node.getMessage();
                if (message != null && node.getRegionDestination() != null) {
                    DeadLetterStrategy deadLetterStrategy = ((Destination) node.getRegionDestination()).getDeadLetterStrategy();
                    if (deadLetterStrategy != null) {
                        if (deadLetterStrategy.isSendToDeadLetterQueue(message)) {
                            ActiveMQDestination deadLetterDestination = deadLetterStrategy.getDeadLetterQueueFor(message, subscription);
                            // Prevent a DLQ loop where same message is sent from a DLQ back to itself
                            if (deadLetterDestination.equals(message.getDestination())) {
                                LOG.debug("Not re-adding to DLQ: {}, dest: {}", message.getMessageId(), message.getDestination());
                                return false;
                            }

                            // message may be inflight to other subscriptions so do not modify
                            message = message.copy();
                            long dlqExpiration = deadLetterStrategy.getExpiration();
                            if (dlqExpiration > 0) {
                                dlqExpiration += System.currentTimeMillis();
                            } else {
                                stampAsExpired(message);
                            }
                            message.setExpiration(dlqExpiration);
                            if (!message.isPersistent()) {
                                message.setPersistent(true);
                                message.setProperty("originalDeliveryMode", "NON_PERSISTENT");
                            }
                            if (poisonCause != null) {
                                message.setProperty(ActiveMQMessage.DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY,
                                        poisonCause.toString());
                            }
                            // The original destination and transaction id do
                            // not get filled when the message is first sent,
                            // it is only populated if the message is routed to
                            // another destination like the DLQ
                            ConnectionContext adminContext = context;
                            if (context.getSecurityContext() == null || !context.getSecurityContext().isBrokerContext()) {
                                adminContext = BrokerSupport.getConnectionContext(this);
                            }
                            addDestination(adminContext, deadLetterDestination, false).getActiveMQDestination().setDLQ(true);
                            BrokerSupport.resendNoCopy(adminContext, message, deadLetterDestination);
                            return true;
                        }
                    } else {
                        LOG.debug("Dead Letter message with no DLQ strategy in place, message id: {}, destination: {}", message.getMessageId(), message.getDestination());
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Caught an exception sending to DLQ: {}", node, e);
        }

        return false;
    }

    @Override
    public Broker getRoot() {
        try {
            return getBrokerService().getBroker();
        } catch (Exception e) {
            LOG.error("Trying to get Root Broker", e);
            throw new RuntimeException("The broker from the BrokerService should not throw an exception");
        }
    }

    /**
     * @return the broker sequence id
     */
    @Override
    public long getBrokerSequenceId() {
        synchronized (sequenceGenerator) {
            return sequenceGenerator.getNextSequenceId();
        }
    }

    @Override
    public Scheduler getScheduler() {
        return this.scheduler;
    }

    @Override
    public ThreadPoolExecutor getExecutor() {
        return this.executor;
    }

    @Override
    public void processConsumerControl(ConsumerBrokerExchange consumerExchange, ConsumerControl control) {
        ActiveMQDestination destination = control.getDestination();
        try {
            getRegion(destination).processConsumerControl(consumerExchange, control);
        } catch (JMSException jmse) {
            LOG.warn("unmatched destination: {}, in consumerControl: {}", destination, control);
        }
    }

    protected void addBrokerInClusterUpdate(BrokerInfo info) {
        List<TransportConnector> connectors = this.brokerService.getTransportConnectors();
        for (TransportConnector connector : connectors) {
            if (connector.isUpdateClusterClients()) {
                connector.addPeerBroker(info);
                connector.updateClientClusterInfo();
            }
        }
    }

    protected void removeBrokerInClusterUpdate(BrokerInfo info) {
        List<TransportConnector> connectors = this.brokerService.getTransportConnectors();
        for (TransportConnector connector : connectors) {
            if (connector.isUpdateClusterClients() && connector.isUpdateClusterClientsOnRemove()) {
                connector.removePeerBroker(info);
                connector.updateClientClusterInfo();
            }
        }
    }

    protected void purgeInactiveDestinations() {
        inactiveDestinationsPurgeLock.writeLock().lock();
        try {
            List<Destination> list = new ArrayList<Destination>();
            Map<ActiveMQDestination, Destination> map = getDestinationMap();
            if (isAllowTempAutoCreationOnSend()) {
                map.putAll(tempQueueRegion.getDestinationMap());
                map.putAll(tempTopicRegion.getDestinationMap());
            }
            long maxPurgedDests = this.brokerService.getMaxPurgedDestinationsPerSweep();
            long timeStamp = System.currentTimeMillis();
            for (Destination d : map.values()) {
                d.markForGC(timeStamp);
                if (d.canGC()) {
                    list.add(d);
                    if (maxPurgedDests > 0 && list.size() == maxPurgedDests) {
                        break;
                    }
                }
            }

            if (!list.isEmpty()) {
                ConnectionContext context = BrokerSupport.getConnectionContext(this);
                context.setBroker(this);

                for (Destination dest : list) {
                    Logger log = LOG;
                    if (dest instanceof BaseDestination) {
                        log = ((BaseDestination) dest).getLog();
                    }
                    log.info("{} Inactive for longer than {} ms - removing ...", dest.getName(), dest.getInactiveTimeoutBeforeGC());
                    try {
                        getRoot().removeDestination(context, dest.getActiveMQDestination(), isAllowTempAutoCreationOnSend() ? 1 : 0);
                    } catch (Throwable e) {
                        LOG.error("Failed to remove inactive destination {}", dest, e);
                    }
                }
            }
        } finally {
            inactiveDestinationsPurgeLock.writeLock().unlock();
        }
    }

    public boolean isAllowTempAutoCreationOnSend() {
        return allowTempAutoCreationOnSend;
    }

    public void setAllowTempAutoCreationOnSend(boolean allowTempAutoCreationOnSend) {
        this.allowTempAutoCreationOnSend = allowTempAutoCreationOnSend;
    }

    @Override
    public void reapplyInterceptor() {
        queueRegion.reapplyInterceptor();
        topicRegion.reapplyInterceptor();
        tempQueueRegion.reapplyInterceptor();
        tempTopicRegion.reapplyInterceptor();
    }
}
