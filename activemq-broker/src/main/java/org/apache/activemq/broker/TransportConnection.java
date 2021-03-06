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
package org.apache.activemq.broker;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.transaction.xa.XAResource;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.region.ConnectionStatistics;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.BrokerSubscriptionInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.CommandTypes;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionError;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ControlCommand;
import org.apache.activemq.command.DataArrayResponse;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.FlushCommand;
import org.apache.activemq.command.IntegerResponse;
import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionId;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.network.DemandForwardingBridge;
import org.apache.activemq.network.MBeanNetworkListener;
import org.apache.activemq.network.NetworkBridgeConfiguration;
import org.apache.activemq.network.NetworkBridgeFactory;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.security.MessageAuthorizationPolicy;
import org.apache.activemq.state.CommandVisitor;
import org.apache.activemq.state.ConnectionState;
import org.apache.activemq.state.ConsumerState;
import org.apache.activemq.state.ProducerState;
import org.apache.activemq.state.SessionState;
import org.apache.activemq.state.TransactionState;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transaction.Transaction;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.TransmitCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportDisposedIOException;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.util.NetworkBridgeUtils;
import org.apache.activemq.util.SubscriptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class TransportConnection implements Connection, Task, CommandVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(TransportConnection.class);
    private static final Logger TRANSPORTLOG = LoggerFactory.getLogger(TransportConnection.class.getName() + ".Transport");
    private static final Logger SERVICELOG = LoggerFactory.getLogger(TransportConnection.class.getName() + ".Service");
    // Keeps track of the broker and connector that created this connection.
    protected final Broker broker;
    protected final BrokerService brokerService;
    protected final TransportConnector connector;
    // Keeps track of the state of the connections.
    // protected final ConcurrentHashMap localConnectionStates=new
    // ConcurrentHashMap();
    //这个集合是region broker里面的集合的引用  所以缓存了一个虚拟机的全部的连接状态
    protected final Map<ConnectionId, ConnectionState> brokerConnectionStates;
    // The broker and wireformat info that was exchanged.
    protected BrokerInfo brokerInfo;
    //看来里面存的是broker发送到客户端的消息
    protected final List<Command> dispatchQueue = new LinkedList<>();
    protected TaskRunner taskRunner;
    protected final AtomicReference<Throwable> transportException = new AtomicReference<>();
    protected AtomicBoolean dispatchStopped = new AtomicBoolean(false);
    //好好看一下是不是每个连接都会有一个新建的这个对象
    private final Transport transport;
    private MessageAuthorizationPolicy messageAuthorizationPolicy;
    private WireFormatInfo wireFormatInfo;
    // Used to do async dispatch.. this should perhaps be pushed down into the
    // transport layer..
    private boolean inServiceException;
    private final ConnectionStatistics statistics = new ConnectionStatistics();
    private boolean manageable;
    private boolean slow;
    private boolean markedCandidate;
    private boolean blockedCandidate;
    private boolean blocked;
    private boolean connected;
    private boolean active;
    //表示正在启动
    private final AtomicBoolean starting = new AtomicBoolean();
    //表示准备关闭
    private final AtomicBoolean pendingStop = new AtomicBoolean();
    private long timeStamp;
    //表示正在关闭中
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    //在关闭之后会把这个东西减一
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final AtomicBoolean asyncException = new AtomicBoolean(false);
    private final Map<ProducerId, ProducerBrokerExchange> producerExchanges = new HashMap<>();
    private final Map<ConsumerId, ConsumerBrokerExchange> consumerExchanges = new HashMap<>();
    private final CountDownLatch dispatchStoppedLatch = new CountDownLatch(1);
    //这个是会变的
    //开启新事务的时候会变
    private ConnectionContext context;
    private boolean networkConnection;
    //为什么可以让一个activemq连接的属性改变这个变量的值
    //这是不是说明了 其实一个物理连接对应一个activemq连接  想想也是蛮合理的
    private boolean faultTolerantConnection;
    //wireformat的版本号
    private final AtomicInteger protocolVersion = new AtomicInteger(CommandTypes.PROTOCOL_VERSION);
    private DemandForwardingBridge duplexBridge;
    private final TaskRunnerFactory taskRunnerFactory;
    private final TaskRunnerFactory stopTaskRunnerFactory;
    //连接状态里面应该包含了所有的会话状态吧
    //这个连接对象是不是每个新的连接都对应一个呢
    private TransportConnectionStateRegister connectionStateRegister = new SingleTransportConnectionStateRegister();
    private final ReentrantReadWriteLock serviceLock = new ReentrantReadWriteLock();
    private String duplexNetworkConnectorId;

    /**
     * @param taskRunnerFactory - can be null if you want direct dispatch to the transport
     *                          else commands are sent async.
     * @param stopTaskRunnerFactory - can <b>not</b> be null, used for stopping this connection.
     */
    public TransportConnection(TransportConnector connector, final Transport transport, Broker broker,
                               TaskRunnerFactory taskRunnerFactory, TaskRunnerFactory stopTaskRunnerFactory) {
        this.connector = connector;
        this.broker = broker;
        this.brokerService = broker.getBrokerService();

        RegionBroker rb = (RegionBroker) broker.getAdaptor(RegionBroker.class);
        brokerConnectionStates = rb.getConnectionStates();
        if (connector != null) {
            this.statistics.setParent(connector.getStatistics());
            this.messageAuthorizationPolicy = connector.getMessageAuthorizationPolicy();
        }
        this.taskRunnerFactory = taskRunnerFactory;
        this.stopTaskRunnerFactory = stopTaskRunnerFactory;
        this.transport = transport;
        if( this.transport instanceof BrokerServiceAware ) {
            ((BrokerServiceAware)this.transport).setBrokerService(brokerService);
        }
        this.transport.setTransportListener(new DefaultTransportListener() {
            @Override
            public void onCommand(Object o) {
                serviceLock.readLock().lock();
                try {
                    if (!(o instanceof Command)) {
                        throw new RuntimeException("Protocol violation - Command corrupted: " + o.toString());
                    }
                    Command command = (Command) o;
                    if (!brokerService.isStopping()) {
                        Response response = service(command);
                        if (response != null && !brokerService.isStopping()) {
                            dispatchSync(response);
                        }
                    } else {
                        throw new BrokerStoppedException("Broker " + brokerService + " is being stopped");
                    }
                } finally {
                    serviceLock.readLock().unlock();
                }
            }

            @Override
            public void onException(IOException exception) {
                serviceLock.readLock().lock();
                try {
                    serviceTransportException(exception);
                } finally {
                    serviceLock.readLock().unlock();
                }
            }
        });
        connected = true;
    }

    /**
     * Returns the number of messages to be dispatched to this connection
     *
     * @return size of dispatch queue
     */
    @Override
    public int getDispatchQueueSize() {
        synchronized (dispatchQueue) {
            return dispatchQueue.size();
        }
    }

    public void serviceTransportException(IOException e) {
        if (!stopping.get() && !pendingStop.get()) {
            transportException.set(e);
            if (TRANSPORTLOG.isDebugEnabled()) {
                TRANSPORTLOG.debug(this + " failed: " + e, e);
            } else if (TRANSPORTLOG.isWarnEnabled() && !expected(e)) {
                TRANSPORTLOG.warn(this + " failed: " + e);
            }
            stopAsync(e);
        }
    }

    private boolean expected(IOException e) {
        return isStomp() && ((e instanceof SocketException && e.getMessage().indexOf("reset") != -1) || e instanceof EOFException);
    }

    private boolean isStomp() {
        URI uri = connector.getUri();
        return uri != null && uri.getScheme() != null && uri.getScheme().indexOf("stomp") != -1;
    }

    /**
     * Calls the serviceException method in an async thread. Since handling a
     * service exception closes a socket, we should not tie up broker threads
     * since client sockets may hang or cause deadlocks.
     */
    @Override
    public void serviceExceptionAsync(final IOException e) {
        if (asyncException.compareAndSet(false, true)) {
            new Thread("Async Exception Handler") {
                @Override
                public void run() {
                    serviceException(e);
                }
            }.start();
        }
    }

    /**
     * Closes a clients connection due to a detected error. Errors are ignored
     * if: the client is closing or broker is closing. Otherwise, the connection
     * error transmitted to the client before stopping it's transport.
     */
    @Override
    public void serviceException(Throwable e) {
        // are we a transport exception such as not being able to dispatch
        // synchronously to a transport
        if (e instanceof IOException) {
            serviceTransportException((IOException) e);
        } else if (e.getClass() == BrokerStoppedException.class) {
            // Handle the case where the broker is stopped
            // But the client is still connected.
            if (!stopping.get()) {
                SERVICELOG.debug("Broker has been stopped.  Notifying client and closing his connection.");
                ConnectionError ce = new ConnectionError();
                ce.setException(e);
                dispatchSync(ce);
                // Record the error that caused the transport to stop
                transportException.set(e);
                // Wait a little bit to try to get the output buffer to flush
                // the exception notification to the client.
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                // Worst case is we just kill the connection before the
                // notification gets to him.
                stopAsync();
            }
        } else if (!stopping.get() && !inServiceException) {
            inServiceException = true;
            try {
                if (SERVICELOG.isDebugEnabled()) {
                    SERVICELOG.debug("Async error occurred: " + e, e);
                } else {
                    SERVICELOG.warn("Async error occurred: " + e);
                }
                ConnectionError ce = new ConnectionError();
                ce.setException(e);
                if (pendingStop.get()) {
                    dispatchSync(ce);
                } else {
                    dispatchAsync(ce);
                }
            } finally {
                inServiceException = false;
            }
        }
    }

    @Override
    //处理消息的进入地方 然后会调用具体的处理
    public Response service(Command command) {
        MDC.put("activemq.connector", connector.getUri().toString());
        Response response = null;
        boolean responseRequired = command.isResponseRequired();
        int commandId = command.getCommandId();
        try {
            if (!pendingStop.get()) {
                response = command.visit(this);
            } else {
                response = new ExceptionResponse(transportException.get());
            }
        } catch (Throwable e) {
            if (SERVICELOG.isDebugEnabled() && e.getClass() != BrokerStoppedException.class) {
                SERVICELOG.debug("Error occured while processing " + (responseRequired ? "sync" : "async")
                        + " command: " + command + ", exception: " + e, e);
            }

            if (e instanceof SuppressReplyException || (e.getCause() instanceof SuppressReplyException)) {
                LOG.info("Suppressing reply to: " + command + " on: " + e + ", cause: " + e.getCause());
                responseRequired = false;
            }

            if (responseRequired) {
                if (e instanceof SecurityException || e.getCause() instanceof SecurityException) {
                    SERVICELOG.warn("Security Error occurred on connection to: {}, {}",
                            transport.getRemoteAddress(), e.getMessage());
                }
                response = new ExceptionResponse(e);
            } else {
                forceRollbackOnlyOnFailedAsyncTransactionOp(e, command);
                serviceException(e);
            }
        }
        if (responseRequired) {
            if (response == null) {
                response = new Response();
            }
            response.setCorrelationId(commandId);
        }
        // The context may have been flagged so that the response is not
        // sent.
        if (context != null) {
            if (context.isDontSendReponse()) {
                context.setDontSendReponse(false);
                response = null;
            }
            context = null;
        }
        MDC.remove("activemq.connector");
        return response;
    }

    private void forceRollbackOnlyOnFailedAsyncTransactionOp(Throwable e, Command command) {
        if (brokerService.isRollbackOnlyOnAsyncException() && !(e instanceof IOException) && isInTransaction(command)) {
            Transaction transaction = getActiveTransaction(command);
            if (transaction != null && !transaction.isRollbackOnly()) {
                LOG.debug("on async exception, force rollback of transaction for: " + command, e);
                transaction.setRollbackOnly(e);
            }
        }
    }

    private Transaction getActiveTransaction(Command command) {
        Transaction transaction = null;
        try {
            if (command instanceof Message) {
                Message messageSend = (Message) command;
                ProducerId producerId = messageSend.getProducerId();
                ProducerBrokerExchange producerExchange = getProducerBrokerExchange(producerId);
                transaction = producerExchange.getConnectionContext().getTransactions().get(messageSend.getTransactionId());
            } else if (command instanceof  MessageAck) {
                MessageAck messageAck = (MessageAck) command;
                ConsumerBrokerExchange consumerExchange = getConsumerBrokerExchange(messageAck.getConsumerId());
                if (consumerExchange != null) {
                    transaction = consumerExchange.getConnectionContext().getTransactions().get(messageAck.getTransactionId());
                }
            }
        } catch(Exception ignored){
            LOG.trace("failed to find active transaction for command: " + command, ignored);
        }
        return transaction;
    }

    private boolean isInTransaction(Command command) {
        return command instanceof Message && ((Message)command).isInTransaction()
                || command instanceof MessageAck && ((MessageAck)command).isInTransaction();
    }

    @Override
    //这应该是处理心跳吧
    public Response processKeepAlive(KeepAliveInfo info) throws Exception {
        return null;
    }

    @Override
    public Response processRemoveSubscription(RemoveSubscriptionInfo info) throws Exception {
        broker.removeSubscription(lookupConnectionState(info.getConnectionId()).getContext(), info);
        return null;
    }

    @Override
    //这看来是最早发送过来的消息啊
    public Response processWireFormat(WireFormatInfo info) throws Exception {
        wireFormatInfo = info;
        protocolVersion.set(info.getVersion());
        return null;
    }

    @Override
    public Response processShutdown(ShutdownInfo info) throws Exception {
        stopAsync();
        return null;
    }

    @Override
    public Response processFlush(FlushCommand command) throws Exception {
        return null;
    }

    //为什么又有提交事务 又有终止事务呢 不是提交事务就相当于终止吗
    @Override
    //返回值都没有的吗
    public Response processBeginTransaction(TransactionInfo info) throws Exception {
        //从这里来看的话 创建连接之后就创建了事务吗  没看到事务包含会话id
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        //为什么新事务的时候需要变呢 不同事务可能是同一个连接创建的嘛
        context = null;
        if (cs != null) {
            //连接状态里面是有连接上下文的
            context = cs.getContext();
        }
        //肯定是先建立了连接之后在新建的事务 所以这里肯定不会为空
        if (cs == null) {
            throw new NullPointerException("Context is null");
        }
        // Avoid replaying dup commands
        //连接状态里面还有事务状态
        if (cs.getTransactionState(info.getTransactionId()) == null) {
            //新建一个事务状态对象放到map里面
            cs.addTransactionState(info.getTransactionId());
            //总结来说就是创建了事务对象 然后存到一个map里面
            broker.beginTransaction(context, info.getTransactionId());
            //事务状态和事务对象直接没有直接的引用关系 但是都有事务id 所以是不是通过事务id来关联这两个对象呢
        }
        return null;
    }

    @Override
    public int getActiveTransactionCount() {
        int rc = 0;
        for (TransportConnectionState cs : connectionStateRegister.listConnectionStates()) {
            rc += cs.getTransactionStates().size();
        }
        return rc;
    }

    @Override
    public Long getOldestActiveTransactionDuration() {
        TransactionState oldestTX = null;
        for (TransportConnectionState cs : connectionStateRegister.listConnectionStates()) {
            Collection<TransactionState> transactions = cs.getTransactionStates();
            for (TransactionState transaction : transactions) {
                if( oldestTX ==null || oldestTX.getCreatedAt() < transaction.getCreatedAt() ) {
                    oldestTX = transaction;
                }
            }
        }
        if( oldestTX == null ) {
            return null;
        }
        return System.currentTimeMillis() - oldestTX.getCreatedAt();
    }

    @Override
    public Response processEndTransaction(TransactionInfo info) throws Exception {
        // No need to do anything. This packet is just sent by the client
        // make sure he is synced with the server as commit command could
        // come from a different connection.
        return null;
    }

    @Override
    public Response processPrepareTransaction(TransactionInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        context = null;
        if (cs != null) {
            context = cs.getContext();
        }
        if (cs == null) {
            throw new NullPointerException("Context is null");
        }
        TransactionState transactionState = cs.getTransactionState(info.getTransactionId());
        if (transactionState == null) {
            throw new IllegalStateException("Cannot prepare a transaction that had not been started or previously returned XA_RDONLY: "
                    + info.getTransactionId());
        }
        // Avoid dups.
        if (!transactionState.isPrepared()) {
            transactionState.setPrepared(true);
            int result = broker.prepareTransaction(context, info.getTransactionId());
            transactionState.setPreparedResult(result);
            if (result == XAResource.XA_RDONLY) {
                // we are done, no further rollback or commit from TM
                cs.removeTransactionState(info.getTransactionId());
            }
            IntegerResponse response = new IntegerResponse(result);
            return response;
        } else {
            IntegerResponse response = new IntegerResponse(transactionState.getPreparedResult());
            return response;
        }
    }

    @Override
    public Response processCommitTransactionOnePhase(TransactionInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        context = cs.getContext();
        cs.removeTransactionState(info.getTransactionId());
        broker.commitTransaction(context, info.getTransactionId(), true);
        return null;
    }

    @Override
    public Response processCommitTransactionTwoPhase(TransactionInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        context = cs.getContext();
        cs.removeTransactionState(info.getTransactionId());
        broker.commitTransaction(context, info.getTransactionId(), false);
        return null;
    }

    @Override
    public Response processRollbackTransaction(TransactionInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        context = cs.getContext();
        cs.removeTransactionState(info.getTransactionId());
        broker.rollbackTransaction(context, info.getTransactionId());
        return null;
    }

    @Override
    public Response processForgetTransaction(TransactionInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        context = cs.getContext();
        broker.forgetTransaction(context, info.getTransactionId());
        return null;
    }

    @Override
    public Response processRecoverTransactions(TransactionInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        context = cs.getContext();
        TransactionId[] preparedTransactions = broker.getPreparedTransactions(context);
        return new DataArrayResponse(preparedTransactions);
    }

    @Override
    //新加入消息
    //怎么感觉topic有点问题呢 每个消息都是每个订阅都有一份 这样的话不是很浪费内存吗
    public Response processMessage(Message messageSend) throws Exception {
        ProducerId producerId = messageSend.getProducerId();
        ProducerBrokerExchange producerExchange = getProducerBrokerExchange(producerId);
        //没什么技术含量
        if (producerExchange.canDispatch(messageSend)) {
            broker.send(producerExchange, messageSend);
        }
        return null;
    }

    @Override
    //消费确认
    public Response processMessageAck(MessageAck ack) throws Exception {
        ConsumerBrokerExchange consumerExchange = getConsumerBrokerExchange(ack.getConsumerId());
        if (consumerExchange != null) {
            broker.acknowledge(consumerExchange, ack);
        } else if (ack.isInTransaction()) {
            LOG.warn("no matching consumer, ignoring ack {}", consumerExchange, ack);
        }
        return null;
    }

    //主动拉消息
    @Override
    public Response processMessagePull(MessagePull pull) throws Exception {
        return broker.messagePull(lookupConnectionState(pull.getConsumerId()).getContext(), pull);
    }

    @Override
    public Response processMessageDispatchNotification(MessageDispatchNotification notification) throws Exception {
        broker.processDispatchNotification(notification);
        return null;
    }

    @Override
    //从后面来看 这里可能会新加同一个地址进来
    public Response processAddDestination(DestinationInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        broker.addDestinationInfo(cs.getContext(), info);
        if (info.getDestination().isTemporary()) {
            cs.addTempDestination(info);
        }
        return null;
    }

    @Override
    public Response processRemoveDestination(DestinationInfo info) throws Exception {
        TransportConnectionState cs = lookupConnectionState(info.getConnectionId());
        broker.removeDestinationInfo(cs.getContext(), info);
        if (info.getDestination().isTemporary()) {
            cs.removeTempDestination(info.getDestination());
        }
        return null;
    }

    @Override
    public Response processAddProducer(ProducerInfo info) throws Exception {
        SessionId sessionId = info.getProducerId().getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        TransportConnectionState cs = lookupConnectionState(connectionId);
        if (cs == null) {
            throw new IllegalStateException("Cannot add a producer to a connection that had not been registered: "
                    + connectionId);
        }
        //添加session的时候有添加过这个
        SessionState ss = cs.getSessionState(sessionId);
        if (ss == null) {
            throw new IllegalStateException("Cannot add a producer to a session that had not been registered: "
                    + sessionId);
        }
        // Avoid replaying dup commands
        if (!ss.getProducerIds().contains(info.getProducerId())) {
            ActiveMQDestination destination = info.getDestination();
            // Do not check for null here as it would cause the count of max producers to exclude
            // anonymous producers.  The isAdvisoryTopic method checks for null so it is safe to
            // call it from here with a null Destination value.
            //地址不是用于存放通知消息时要做的处理
            //只是判断了一下一个连接下的消费者数量是否超过最大值
            if (!AdvisorySupport.isAdvisoryTopic(destination)) {
                if (getProducerCount(connectionId) >= connector.getMaximumProducersAllowedPerConnection()){
                    throw new IllegalStateException("Can't add producer on connection " + connectionId + ": at maximum limit: " + connector.getMaximumProducersAllowedPerConnection());
                }
            }
            broker.addProducer(cs.getContext(), info);
            try {
                //就是新加了一个消费者状态对象
                ss.addProducer(info);
            } catch (IllegalStateException e) {
                broker.removeProducer(cs.getContext(), info);
            }

        }
        return null;
    }

    @Override
    public Response processRemoveProducer(ProducerId id) throws Exception {
        SessionId sessionId = id.getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        TransportConnectionState cs = lookupConnectionState(connectionId);
        SessionState ss = cs.getSessionState(sessionId);
        if (ss == null) {
            throw new IllegalStateException("Cannot remove a producer from a session that had not been registered: "
                    + sessionId);
        }
        ProducerState ps = ss.removeProducer(id);
        if (ps == null) {
            throw new IllegalStateException("Cannot remove a producer that had not been registered: " + id);
        }
        removeProducerBrokerExchange(id);
        broker.removeProducer(cs.getContext(), ps.getInfo());
        return null;
    }

    @Override
    public Response processAddConsumer(ConsumerInfo info) throws Exception {
        SessionId sessionId = info.getConsumerId().getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        TransportConnectionState cs = lookupConnectionState(connectionId);
        if (cs == null) {
            throw new IllegalStateException("Cannot add a consumer to a connection that had not been registered: "
                    + connectionId);
        }
        SessionState ss = cs.getSessionState(sessionId);
        if (ss == null) {
            throw new IllegalStateException(broker.getBrokerName()
                    + " Cannot add a consumer to a session that had not been registered: " + sessionId);
        }
        // Avoid replaying dup commands
        //只有新的消费者加进来才处理
        if (!ss.getConsumerIds().contains(info.getConsumerId())) {
            ActiveMQDestination destination = info.getDestination();
            if (destination != null && !AdvisorySupport.isAdvisoryTopic(destination)) {
                if (getConsumerCount(connectionId) >= connector.getMaximumConsumersAllowedPerConnection()){
                    throw new IllegalStateException("Can't add consumer on connection " + connectionId + ": at maximum limit: " + connector.getMaximumConsumersAllowedPerConnection());
                }
            }

            broker.addConsumer(cs.getContext(), info);
            try {
                //只是加一个状态对象
                ss.addConsumer(info);
                addConsumerBrokerExchange(cs, info.getConsumerId());
            } catch (IllegalStateException e) {
                broker.removeConsumer(cs.getContext(), info);
            }

        }
        return null;
    }

    @Override
    public Response processRemoveConsumer(ConsumerId id, long lastDeliveredSequenceId) throws Exception {
        SessionId sessionId = id.getParentId();
        ConnectionId connectionId = sessionId.getParentId();
        TransportConnectionState cs = lookupConnectionState(connectionId);
        if (cs == null) {
            throw new IllegalStateException("Cannot remove a consumer from a connection that had not been registered: "
                    + connectionId);
        }
        SessionState ss = cs.getSessionState(sessionId);
        if (ss == null) {
            throw new IllegalStateException("Cannot remove a consumer from a session that had not been registered: "
                    + sessionId);
        }
        ConsumerState consumerState = ss.removeConsumer(id);
        if (consumerState == null) {
            throw new IllegalStateException("Cannot remove a consumer that had not been registered: " + id);
        }
        ConsumerInfo info = consumerState.getInfo();
        info.setLastDeliveredSequenceId(lastDeliveredSequenceId);
        broker.removeConsumer(cs.getContext(), consumerState.getInfo());
        removeConsumerBrokerExchange(id);
        return null;
    }

    @Override
    //难以想象的简单啊
    public Response processAddSession(SessionInfo info) throws Exception {
        ConnectionId connectionId = info.getSessionId().getParentId();
        TransportConnectionState cs = lookupConnectionState(connectionId);
        // Avoid replaying dup commands
        if (cs != null && !cs.getSessionIds().contains(info.getSessionId())) {
            //什么都没做
            broker.addSession(cs.getContext(), info);
            try {
                cs.addSession(info);
            } catch (IllegalStateException e) {
                LOG.warn("Failed to add session: {}", info.getSessionId(), e);
                broker.removeSession(cs.getContext(), info);
            }
        }
        return null;
    }

    @Override
    public Response processRemoveSession(SessionId id, long lastDeliveredSequenceId) throws Exception {
        ConnectionId connectionId = id.getParentId();
        TransportConnectionState cs = lookupConnectionState(connectionId);
        if (cs == null) {
            throw new IllegalStateException("Cannot remove session from connection that had not been registered: " + connectionId);
        }
        SessionState session = cs.getSessionState(id);
        if (session == null) {
            throw new IllegalStateException("Cannot remove session that had not been registered: " + id);
        }
        // Don't let new consumers or producers get added while we are closing
        // this down.
        session.shutdown();
        // Cascade the connection stop to the consumers and producers.
        for (ConsumerId consumerId : session.getConsumerIds()) {
            try {
                processRemoveConsumer(consumerId, lastDeliveredSequenceId);
            } catch (Throwable e) {
                LOG.warn("Failed to remove consumer: {}", consumerId, e);
            }
        }
        for (ProducerId producerId : session.getProducerIds()) {
            try {
                processRemoveProducer(producerId);
            } catch (Throwable e) {
                LOG.warn("Failed to remove producer: {}", producerId, e);
            }
        }
        cs.removeSession(id);
        broker.removeSession(cs.getContext(), session.getInfo());
        return null;
    }

    @Override
    public Response processAddConnection(ConnectionInfo info) throws Exception {
        // Older clients should have been defaulting this field to true.. but
        // they were not.
        if (wireFormatInfo != null && wireFormatInfo.getVersion() <= 2) {
            //这是什么 看起来像是集群一样的
            //默认就是true啊  难道什么时候设置为false了吗
            info.setClientMaster(true);
        }
        //每个连接都有一个与之对应的状态对象吧
        TransportConnectionState state;
        // Make sure 2 concurrent connections by the same ID only generate 1
        // TransportConnectionState object.
        //同一个连接来多次 会直接删掉之前的连接状态
        synchronized (brokerConnectionStates) {
            state = (TransportConnectionState) brokerConnectionStates.get(info.getConnectionId());
            if (state == null) {
                //一个具体的连接应该可以创建多个activemq连接吧
                state = new TransportConnectionState(info, this);
                brokerConnectionStates.put(info.getConnectionId(), state);
            }
            //难道还会一个连接id会被多次创建吗
            state.incrementReference();
        }
        // If there are 2 concurrent connections for the same connection id,
        // then last one in wins, we need to sync here
        // to figure out the winner.
        synchronized (state.getConnectionMutex()) {
            //怎么会不相等呢 难道说还有从其他连接同步过来的状态吗
            if (state.getConnection() != this) {
                LOG.debug("Killing previous stale connection: {}", state.getConnection().getRemoteAddress());
                state.getConnection().stop();
                LOG.debug("Connection {} taking over previous connection: {}", getRemoteAddress(), state.getConnection().getRemoteAddress());
                state.setConnection(this);
                state.reset(info);
            }
        }
        //会返回之前注册的状态对象
        registerConnectionState(info.getConnectionId(), state);
        LOG.debug("Setting up new connection id: {}, address: {}, info: {}", new Object[]{ info.getConnectionId(), getRemoteAddress(), info });
        //这个连接是socket初始化的连接 会把客户端新建的消息连接的错误容忍属性赋给这个transport连接
        //为什么已经放在连接上下文了 还要给这个transport connection也赋值
        this.faultTolerantConnection = info.isFaultTolerant();
        // Setup the context.
        //每个连接才有一个客户端id
        //客户端id是什么呢  是不是一个具体的机器对应一个id
        String clientId = info.getClientId();
        context = new ConnectionContext();
        context.setBroker(broker);
        context.setClientId(clientId);
        context.setClientMaster(info.isClientMaster());
        context.setConnection(this);
        context.setConnectionId(info.getConnectionId());
        context.setConnector(connector);
        //目前而言肯定是个null 需要使用者自己去实现的 可以看一下怎么用的
        context.setMessageAuthorizationPolicy(getMessageAuthorizationPolicy());
        //网络连接会是什么呢
        context.setNetworkConnection(networkConnection);
        context.setFaultTolerant(faultTolerantConnection);
        context.setTransactions(new ConcurrentHashMap<TransactionId, Transaction>());
        context.setUserName(info.getUserName());
        context.setWireFormatInfo(wireFormatInfo);
        context.setReconnect(info.isFailoverReconnect());
        this.manageable = info.isManageable();
        context.setConnectionState(state);
        state.setContext(context);
        state.setConnection(this);
        if (info.getClientIp() == null) {
            info.setClientIp(getRemoteAddress());
        }

        try {
            broker.addConnection(context, info);
        } catch (Exception e) {
            synchronized (brokerConnectionStates) {
                brokerConnectionStates.remove(info.getConnectionId());
            }
            unregisterConnectionState(info.getConnectionId());
            LOG.warn("Failed to add Connection id={}, clientId={} due to {}", info.getConnectionId(), clientId, e.getLocalizedMessage(), e);
            //AMQ-6561 - stop for all exceptions on addConnection
            // close this down - in case the peer of this transport doesn't play nice
            delayedStop(2000, "Failed with SecurityException: " + e.getLocalizedMessage(), e);
            throw e;
        }
        //这是什么意思 连接是否能够被控制吗
        if (info.isManageable()) {
            // send ConnectionCommand
            //这个东西本身好像是不干嘛的  只是一种消息 实现的功能还是靠消息处理的时候进行相应的操作
            ConnectionControl command = this.connector.getConnectionControl();
            command.setFaultTolerant(broker.isFaultTolerantConfiguration());
            //为什么是错误重连的就不能够是负载均衡的呢
            if (info.isFailoverReconnect()) {
                command.setRebalanceConnection(false);
            }
            //这里已经是broker内部了  消息发给谁呢 为什么不直接调相应方法呢
            //根据代码猜测 应该是把消息写到了磁盘 然后将消息发送给客户端
            //一个大胆的猜测:消息发送的时候会从磁盘取出这个消息 然后根据里面的数据决定消息发送的客户端顺序 至于为什么吧这个消息发送个客户端 目前还不是很明白
            dispatchAsync(command);
        }
        return null;
    }

    @Override
    public synchronized Response processRemoveConnection(ConnectionId id, long lastDeliveredSequenceId)
            throws InterruptedException {
        LOG.debug("remove connection id: {}", id);
        TransportConnectionState cs = lookupConnectionState(id);
        if (cs != null) {
            // Don't allow things to be added to the connection state while we
            // are shutting down.
            cs.shutdown();
            // Cascade the connection stop to the sessions.
            for (SessionId sessionId : cs.getSessionIds()) {
                try {
                    processRemoveSession(sessionId, lastDeliveredSequenceId);
                } catch (Throwable e) {
                    SERVICELOG.warn("Failed to remove session {}", sessionId, e);
                }
            }
            // Cascade the connection stop to temp destinations.
            for (Iterator<DestinationInfo> iter = cs.getTempDestinations().iterator(); iter.hasNext(); ) {
                DestinationInfo di = iter.next();
                try {
                    broker.removeDestination(cs.getContext(), di.getDestination(), 0);
                } catch (Throwable e) {
                    SERVICELOG.warn("Failed to remove tmp destination {}", di.getDestination(), e);
                }
                iter.remove();
            }
            try {
                broker.removeConnection(cs.getContext(), cs.getInfo(), transportException.get());
            } catch (Throwable e) {
                SERVICELOG.warn("Failed to remove connection {}", cs.getInfo(), e);
            }
            TransportConnectionState state = unregisterConnectionState(id);
            if (state != null) {
                synchronized (brokerConnectionStates) {
                    // If we are the last reference, we should remove the state
                    // from the broker.
                    if (state.decrementReference() == 0) {
                        brokerConnectionStates.remove(id);
                    }
                }
            }
        }
        return null;
    }

    @Override
    public Response processProducerAck(ProducerAck ack) throws Exception {
        // A broker should not get ProducerAck messages.
        return null;
    }

    @Override
    public Connector getConnector() {
        return connector;
    }

    @Override
    public void dispatchSync(Command message) {
        try {
            processDispatch(message);
        } catch (IOException e) {
            serviceExceptionAsync(e);
        }
    }

    @Override
    public void dispatchAsync(Command message) {
        if (!stopping.get()) {
            if (taskRunner == null) {
                //没有线程池的话就直接发送
                dispatchSync(message);
            } else {
                synchronized (dispatchQueue) {
                    dispatchQueue.add(message);
                }
                try {
                    taskRunner.wakeup();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } else {//这个连接正在被关闭
            if (message.isMessageDispatch()) {
                MessageDispatch md = (MessageDispatch) message;
                TransmitCallback sub = md.getTransmitCallback();
                broker.postProcessDispatch(md);
                if (sub != null) {
                    sub.onFailure();
                }
            }
        }
    }

    protected void processDispatch(Command command) throws IOException {
        MessageDispatch messageDispatch = (MessageDispatch) (command.isMessageDispatch() ? command : null);
        try {
            if (!stopping.get()) {
                if (messageDispatch != null) {
                    try {
                        broker.preProcessDispatch(messageDispatch);
                    } catch (RuntimeException convertToIO) {
                        throw new IOException(convertToIO);
                    }
                }
                dispatch(command);
            }
        } catch (IOException e) {
            if (messageDispatch != null) {
                TransmitCallback sub = messageDispatch.getTransmitCallback();
                broker.postProcessDispatch(messageDispatch);
                if (sub != null) {
                    sub.onFailure();
                }
                messageDispatch = null;
                throw e;
            } else {
                if (TRANSPORTLOG.isDebugEnabled()) {
                    TRANSPORTLOG.debug("Unexpected exception on asyncDispatch, command of type: " + command.getDataStructureType(), e);
                }
            }
        } finally {
            if (messageDispatch != null) {
                TransmitCallback sub = messageDispatch.getTransmitCallback();
                broker.postProcessDispatch(messageDispatch);
                if (sub != null) {
                    sub.onSuccess();
                }
            }
        }
    }

    @Override
    public boolean iterate() {
        try {
            if (pendingStop.get() || stopping.get()) {
                if (dispatchStopped.compareAndSet(false, true)) {
                    if (transportException.get() == null) {
                        try {
                            dispatch(new ShutdownInfo());
                        } catch (Throwable ignore) {
                        }
                    }
                    dispatchStoppedLatch.countDown();
                }
                return false;
            }
            if (!dispatchStopped.get()) {
                Command command = null;
                synchronized (dispatchQueue) {
                    if (dispatchQueue.isEmpty()) {
                        return false;
                    }
                    command = dispatchQueue.remove(0);
                }
                processDispatch(command);
                return true;
            }
            return false;
        } catch (IOException e) {
            if (dispatchStopped.compareAndSet(false, true)) {
                dispatchStoppedLatch.countDown();
            }
            serviceExceptionAsync(e);
            return false;
        }
    }

    /**
     * Returns the statistics for this connection
     */
    @Override
    public ConnectionStatistics getStatistics() {
        return statistics;
    }

    public MessageAuthorizationPolicy getMessageAuthorizationPolicy() {
        return messageAuthorizationPolicy;
    }

    public void setMessageAuthorizationPolicy(MessageAuthorizationPolicy messageAuthorizationPolicy) {
        this.messageAuthorizationPolicy = messageAuthorizationPolicy;
    }

    @Override
    public boolean isManageable() {
        return manageable;
    }

    @Override
    public void start() throws Exception {
        try {
            synchronized (this) {
                starting.set(true);
                if (taskRunnerFactory != null) {
                    taskRunner = taskRunnerFactory.createTaskRunner(this, "ActiveMQ Connection Dispatcher: "
                            + getRemoteAddress());
                } else {
                    taskRunner = null;
                }
                transport.start();
                active = true;
                BrokerInfo info = connector.getBrokerInfo().copy();
                if (connector.isUpdateClusterClients()) {
                    info.setPeerBrokerInfos(this.broker.getPeerBrokerInfos());
                } else {
                    info.setPeerBrokerInfos(null);
                }
                dispatchAsync(info);

                connector.onStarted(this);
            }
        } catch (Exception e) {
            // Force clean up on an error starting up.
            pendingStop.set(true);
            throw e;
        } finally {
            // stop() can be called from within the above block,
            // but we want to be sure start() completes before
            // stop() runs, so queue the stop until right now:
            setStarting(false);
            if (isPendingStop()) {
                LOG.debug("Calling the delayed stop() after start() {}", this);
                stop();
            }
        }
    }

    @Override
    public void stop() throws Exception {
        // do not stop task the task runner factories (taskRunnerFactory, stopTaskRunnerFactory)
        // as their lifecycle is handled elsewhere

        stopAsync();
        while (!stopped.await(5, TimeUnit.SECONDS)) {
            LOG.info("The connection to '{}' is taking a long time to shutdown.", transport.getRemoteAddress());
        }
    }

    public void delayedStop(final int waitTime, final String reason, Throwable cause) {
        if (waitTime > 0) {
            synchronized (this) {
                pendingStop.set(true);
                transportException.set(cause);
            }
            try {
                stopTaskRunnerFactory.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Thread.sleep(waitTime);
                            stopAsync();
                            LOG.info("Stopping {} because {}", transport.getRemoteAddress(), reason);
                        } catch (InterruptedException e) {
                        }
                    }
                });
            } catch (Throwable t) {
                LOG.warn("Cannot create stopAsync. This exception will be ignored.", t);
            }
        }
    }

    //还传个原因是干嘛呢  难道是为了打印吗
    public void stopAsync(Throwable cause) {
        transportException.set(cause);
        stopAsync();
    }

    public void stopAsync() {
        // If we're in the middle of starting then go no further... for now.
        synchronized (this) {
            pendingStop.set(true);
            //表示正在启动
            //能够直接返回说明启动的时候应该会检查pendingStop 如果为true就会停止启动
            if (starting.get()) {
                LOG.debug("stopAsync() called in the middle of start(). Delaying till start completes..");
                return;
            }
        }
        //如果正在被关就不处理
        //一个connect会做什么呢 不停的读取数据然后处理数据吗
        if (stopping.compareAndSet(false, true)) {
            // Let all the connection contexts know we are shutting down
            // so that in progress operations can notice and unblock.
            //不出意料就是获取所有的连接id,但是我只是关闭一个连接通知所以连接关闭是几个意思
            //分析来讲这是不可能的 所以说应该就是返回了一个连接状态,map的那个register不知道是什么时候用的
            List<TransportConnectionState> connectionStates = listConnectionStates();
            //这种foreach循环好像底层会被翻译成通过迭代器不停的取数据
            for (TransportConnectionState cs : connectionStates) {
                ConnectionContext connectionContext = cs.getContext();
                if (connectionContext != null) {
                    //连接上下文做了什么事吗 应该是有其他东西在用这个标志来判断吧
                    connectionContext.getStopping().set(true);
                }
            }
            try {
                stopTaskRunnerFactory.execute(new Runnable() {
                    @Override
                    public void run() {
                        serviceLock.writeLock().lock();
                        try {
                            doStop();
                        } catch (Throwable e) {
                            LOG.debug("Error occurred while shutting down a connection {}", this, e);
                        } finally {
                            stopped.countDown();
                            serviceLock.writeLock().unlock();
                        }
                    }
                });
            } catch (Throwable t) {
                LOG.warn("Cannot create async transport stopper thread. This exception is ignored. Not waiting for stop to complete", t);
                stopped.countDown();
            }
        }
    }

    @Override
    public String toString() {
        return "Transport Connection to: " + transport.getRemoteAddress();
    }

    protected void doStop() throws Exception {
        LOG.debug("Stopping connection: {}", transport.getRemoteAddress());
        //就是简单的从一个集合里面删除这个连接
        connector.onStopped(this);
        try {
            synchronized (this) {
                if (duplexBridge != null) {
                    //看名字像是一个用于备份的对象 todo 留待以后研究
                    duplexBridge.stop();
                }
            }
        } catch (Exception ignore) {
            LOG.trace("Exception caught stopping. This exception is ignored.", ignore);
        }
        try {
            //在逗我的吧 停一个连接 怎么会停transport
            //todo 需要看一下transport是不是每个broker只有一个还是每个连接都会创建一个新的
            //感觉这个transport应该是一个transport server收到新的连接之后 新建了一个transport client对象
            transport.stop();
            LOG.debug("Stopped transport: {}", transport.getRemoteAddress());
        } catch (Exception e) {
            LOG.debug("Could not stop transport to {}. This exception is ignored.", transport.getRemoteAddress(), e);
        }
        if (taskRunner != null) {
            //nb啊 就给了一秒钟去关线程池
            //可以学习一下别人是怎么关闭线程池的 主要是看线程池关闭的时候怎么等待所有提交的任务完成
            //这里用的根本不是线程池
            taskRunner.shutdown(1);
            taskRunner = null;
        }
        active = false;
        // Run the MessageDispatch callbacks so that message references get
        // cleaned up.
        synchronized (dispatchQueue) {
            //这个里面放的其他命令就被丢了吗
            //这个里面的命令是客户端发给broker的还是broker发给客户端的
            for (Iterator<Command> iter = dispatchQueue.iterator(); iter.hasNext(); ) {
                Command command = iter.next();
                if (command.isMessageDispatch()) {
                    MessageDispatch md = (MessageDispatch) command;
                    TransmitCallback sub = md.getTransmitCallback();
                    //什么都没做
                    broker.postProcessDispatch(md);
                    if (sub != null) {
                        //不知道在干嘛 等看客户端拉消息的时候在研究
                        sub.onFailure();
                    }
                }
            }
            dispatchQueue.clear();
        }
        //
        // Remove all logical connection associated with this connection
        // from the broker.
        if (!broker.isStopped()) {
            //这个不是之前做过了吗
            List<TransportConnectionState> connectionStates = listConnectionStates();
            connectionStates = listConnectionStates();
            for (TransportConnectionState cs : connectionStates) {
                cs.getContext().getStopping().set(true);
                try {
                    LOG.debug("Cleaning up connection resources: {}", getRemoteAddress());
                    processRemoveConnection(cs.getInfo().getConnectionId(), RemoveInfo.LAST_DELIVERED_UNKNOWN);
                } catch (Throwable ignore) {
                    LOG.debug("Exception caught removing connection {}. This exception is ignored.", cs.getInfo().getConnectionId(), ignore);
                }
            }
        }
        LOG.debug("Connection Stopped: {}", getRemoteAddress());
    }

    /**
     * @return Returns the blockedCandidate.
     */
    public boolean isBlockedCandidate() {
        return blockedCandidate;
    }

    /**
     * @param blockedCandidate The blockedCandidate to set.
     */
    public void setBlockedCandidate(boolean blockedCandidate) {
        this.blockedCandidate = blockedCandidate;
    }

    /**
     * @return Returns the markedCandidate.
     */
    public boolean isMarkedCandidate() {
        return markedCandidate;
    }

    /**
     * @param markedCandidate The markedCandidate to set.
     */
    public void setMarkedCandidate(boolean markedCandidate) {
        this.markedCandidate = markedCandidate;
        if (!markedCandidate) {
            timeStamp = 0;
            blockedCandidate = false;
        }
    }

    /**
     * @param slow The slow to set.
     */
    public void setSlow(boolean slow) {
        this.slow = slow;
    }

    /**
     * @return true if the Connection is slow
     */
    @Override
    public boolean isSlow() {
        return slow;
    }

    /**
     * @return true if the Connection is potentially blocked
     */
    public boolean isMarkedBlockedCandidate() {
        return markedCandidate;
    }

    /**
     * Mark the Connection, so we can deem if it's collectable on the next sweep
     */
    public void doMark() {
        if (timeStamp == 0) {
            timeStamp = System.currentTimeMillis();
        }
    }

    /**
     * @return if after being marked, the Connection is still writing
     */
    @Override
    public boolean isBlocked() {
        return blocked;
    }

    /**
     * @return true if the Connection is connected
     */
    @Override
    public boolean isConnected() {
        return connected;
    }

    /**
     * @param blocked The blocked to set.
     */
    public void setBlocked(boolean blocked) {
        this.blocked = blocked;
    }

    /**
     * @param connected The connected to set.
     */
    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    /**
     * @return true if the Connection is active
     */
    @Override
    public boolean isActive() {
        return active;
    }

    /**
     * @param active The active to set.
     */
    public void setActive(boolean active) {
        this.active = active;
    }

    /**
     * @return true if the Connection is starting
     */
    public boolean isStarting() {
        return starting.get();
    }

    @Override
    public synchronized boolean isNetworkConnection() {
        return networkConnection;
    }

    @Override
    public boolean isFaultTolerantConnection() {
        return this.faultTolerantConnection;
    }

    protected void setStarting(boolean starting) {
        this.starting.set(starting);
    }

    /**
     * @return true if the Connection needs to stop
     */
    public boolean isPendingStop() {
        return pendingStop.get();
    }

    protected void setPendingStop(boolean pendingStop) {
        this.pendingStop.set(pendingStop);
    }

    private NetworkBridgeConfiguration getNetworkConfiguration(final BrokerInfo info) throws IOException {
        Properties properties = MarshallingSupport.stringToProperties(info.getNetworkProperties());
        Map<String, String> props = createMap(properties);
        NetworkBridgeConfiguration config = new NetworkBridgeConfiguration();
        IntrospectionSupport.setProperties(config, props, "");
        return config;
    }

    @Override
    public Response processBrokerInfo(BrokerInfo info) {
        if (info.isSlaveBroker()) {
            LOG.error(" Slave Brokers are no longer supported - slave trying to attach is: {}", info.getBrokerName());
        } else if (info.isNetworkConnection() && !info.isDuplexConnection()) {
            try {
                NetworkBridgeConfiguration config = getNetworkConfiguration(info);
                if (config.isSyncDurableSubs() && protocolVersion.get() >= CommandTypes.PROTOCOL_VERSION_DURABLE_SYNC) {
                    LOG.debug("SyncDurableSubs is enabled, Sending BrokerSubscriptionInfo");
                    dispatchSync(NetworkBridgeUtils.getBrokerSubscriptionInfo(this.broker.getBrokerService(), config));
                }
            } catch (Exception e) {
                LOG.error("Failed to respond to network bridge creation from broker {}", info.getBrokerId(), e);
                return null;
            }
        } else if (info.isNetworkConnection() && info.isDuplexConnection()) {
            // so this TransportConnection is the rear end of a network bridge
            // We have been requested to create a two way pipe ...
            try {
                NetworkBridgeConfiguration config = getNetworkConfiguration(info);
                config.setBrokerName(broker.getBrokerName());

                if (config.isSyncDurableSubs() && protocolVersion.get() >= CommandTypes.PROTOCOL_VERSION_DURABLE_SYNC) {
                    LOG.debug("SyncDurableSubs is enabled, Sending BrokerSubscriptionInfo");
                    dispatchSync(NetworkBridgeUtils.getBrokerSubscriptionInfo(this.broker.getBrokerService(), config));
                }

                // check for existing duplex connection hanging about

                // We first look if existing network connection already exists for the same broker Id and network connector name
                // It's possible in case of brief network fault to have this transport connector side of the connection always active
                // and the duplex network connector side wanting to open a new one
                // In this case, the old connection must be broken
                String duplexNetworkConnectorId = config.getName() + "@" + info.getBrokerId();
                CopyOnWriteArrayList<TransportConnection> connections = this.connector.getConnections();
                synchronized (connections) {
                    for (Iterator<TransportConnection> iter = connections.iterator(); iter.hasNext(); ) {
                        TransportConnection c = iter.next();
                        if ((c != this) && (duplexNetworkConnectorId.equals(c.getDuplexNetworkConnectorId()))) {
                            LOG.warn("Stopping an existing active duplex connection [{}] for network connector ({}).", c, duplexNetworkConnectorId);
                            c.stopAsync();
                            // better to wait for a bit rather than get connection id already in use and failure to start new bridge
                            c.getStopped().await(1, TimeUnit.SECONDS);
                        }
                    }
                    setDuplexNetworkConnectorId(duplexNetworkConnectorId);
                }
                Transport localTransport = NetworkBridgeFactory.createLocalTransport(config, broker.getVmConnectorURI());
                Transport remoteBridgeTransport = transport;
                if (! (remoteBridgeTransport instanceof ResponseCorrelator)) {
                    // the vm transport case is already wrapped
                    remoteBridgeTransport = new ResponseCorrelator(remoteBridgeTransport);
                }
                String duplexName = localTransport.toString();
                if (duplexName.contains("#")) {
                    duplexName = duplexName.substring(duplexName.lastIndexOf("#"));
                }
                MBeanNetworkListener listener = new MBeanNetworkListener(brokerService, config, brokerService.createDuplexNetworkConnectorObjectName(duplexName));
                listener.setCreatedByDuplex(true);
                duplexBridge = config.getBridgeFactory().createNetworkBridge(config, localTransport, remoteBridgeTransport, listener);
                duplexBridge.setBrokerService(brokerService);
                //Need to set durableDestinations to properly restart subs when dynamicOnly=false
                duplexBridge.setDurableDestinations(NetworkConnector.getDurableTopicDestinations(
                        broker.getDurableDestinations()));

                // now turn duplex off this side
                info.setDuplexConnection(false);
                duplexBridge.setCreatedByDuplex(true);
                duplexBridge.duplexStart(this, brokerInfo, info);
                LOG.info("Started responder end of duplex bridge {}", duplexNetworkConnectorId);
                return null;
            } catch (TransportDisposedIOException e) {
                LOG.warn("Duplex bridge {} was stopped before it was correctly started.", duplexNetworkConnectorId);
                return null;
            } catch (Exception e) {
                LOG.error("Failed to create responder end of duplex network bridge {}", duplexNetworkConnectorId, e);
                return null;
            }
        }
        // We only expect to get one broker info command per connection
        if (this.brokerInfo != null) {
            LOG.warn("Unexpected extra broker info command received: {}", info);
        }
        this.brokerInfo = info;
        networkConnection = true;
        List<TransportConnectionState> connectionStates = listConnectionStates();
        for (TransportConnectionState cs : connectionStates) {
            cs.getContext().setNetworkConnection(true);
        }
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private HashMap<String, String> createMap(Properties properties) {
        return new HashMap(properties);
    }

    protected void dispatch(Command command) throws IOException {
        try {
            setMarkedCandidate(true);
            transport.oneway(command);
        } finally {
            setMarkedCandidate(false);
        }
    }

    @Override
    public String getRemoteAddress() {
        return transport.getRemoteAddress();
    }

    public Transport getTransport() {
        return transport;
    }

    @Override
    public String getConnectionId() {
        List<TransportConnectionState> connectionStates = listConnectionStates();
        for (TransportConnectionState cs : connectionStates) {
            if (cs.getInfo().getClientId() != null) {
                return cs.getInfo().getClientId();
            }
            return cs.getInfo().getConnectionId().toString();
        }
        return null;
    }

    @Override
    public void updateClient(ConnectionControl control) {
        if (isActive() && isBlocked() == false && isFaultTolerantConnection() && this.wireFormatInfo != null
                && this.wireFormatInfo.getVersion() >= 6) {
            dispatchAsync(control);
        }
    }

    public ProducerBrokerExchange getProducerBrokerExchangeIfExists(ProducerInfo producerInfo){
        ProducerBrokerExchange result = null;
        if (producerInfo != null && producerInfo.getProducerId() != null){
            synchronized (producerExchanges){
                result = producerExchanges.get(producerInfo.getProducerId());
            }
        }
        return result;
    }

    //得到一个综合几个变量的变量
    //每个生产者对应一个这个对象
    //transportconnection是每个连接对应一个 一个连接可能会创建多个生产者 所以说这里会有几何来缓存这个对象和生产者的对应关系
    private ProducerBrokerExchange getProducerBrokerExchange(ProducerId id) throws IOException {
        ProducerBrokerExchange result = producerExchanges.get(id);
        if (result == null) {
            synchronized (producerExchanges) {
                //构造函数没做什么事 看来那些变量都是后来手动设置的
                result = new ProducerBrokerExchange();
                //连接状态和连接是一对一的关系
                TransportConnectionState state = lookupConnectionState(id);
                //获取连接上下文
                //添加连接的时候会把连接上下文对象放进缓存
                context = state.getContext();
                result.setConnectionContext(context);
                //
                if (context.isReconnect() || (context.isNetworkConnection() && connector.isAuditNetworkProducers())) {
                    result.setLastStoredSequenceId(brokerService.getPersistenceAdapter().getLastProducerSequenceId(id));
                }
                SessionState ss = state.getSessionState(id.getParentId());
                if (ss != null) {
                    result.setProducerState(ss.getProducerState(id));
                    ProducerState producerState = ss.getProducerState(id);
                    if (producerState != null && producerState.getInfo() != null) {
                        ProducerInfo info = producerState.getInfo();
                        result.setMutable(info.getDestination() == null || info.getDestination().isComposite());
                    }
                }
                producerExchanges.put(id, result);
            }
        } else {
            context = result.getConnectionContext();
        }
        return result;
    }

    private void removeProducerBrokerExchange(ProducerId id) {
        synchronized (producerExchanges) {
            producerExchanges.remove(id);
        }
    }

    private ConsumerBrokerExchange getConsumerBrokerExchange(ConsumerId id) {
        ConsumerBrokerExchange result = consumerExchanges.get(id);
        return result;
    }

    private ConsumerBrokerExchange addConsumerBrokerExchange(TransportConnectionState connectionState, ConsumerId id) {
        ConsumerBrokerExchange result = consumerExchanges.get(id);
        if (result == null) {
            synchronized (consumerExchanges) {
                result = new ConsumerBrokerExchange();
                context = connectionState.getContext();
                result.setConnectionContext(context);
                SessionState ss = connectionState.getSessionState(id.getParentId());
                if (ss != null) {
                    ConsumerState cs = ss.getConsumerState(id);
                    if (cs != null) {
                        ConsumerInfo info = cs.getInfo();
                        if (info != null) {
                            if (info.getDestination() != null && info.getDestination().isPattern()) {
                                result.setWildcard(true);
                            }
                        }
                    }
                }
                consumerExchanges.put(id, result);
            }
        }
        return result;
    }

    private void removeConsumerBrokerExchange(ConsumerId id) {
        synchronized (consumerExchanges) {
            consumerExchanges.remove(id);
        }
    }

    public int getProtocolVersion() {
        return protocolVersion.get();
    }

    @Override
    public Response processControlCommand(ControlCommand command) throws Exception {
        return null;
    }

    @Override
    public Response processMessageDispatch(MessageDispatch dispatch) throws Exception {
        return null;
    }

    @Override
    public Response processConnectionControl(ConnectionControl control) throws Exception {
        if (control != null) {
            faultTolerantConnection = control.isFaultTolerant();
        }
        return null;
    }

    @Override
    public Response processConnectionError(ConnectionError error) throws Exception {
        return null;
    }

    @Override
    public Response processConsumerControl(ConsumerControl control) throws Exception {
        ConsumerBrokerExchange consumerExchange = getConsumerBrokerExchange(control.getConsumerId());
        broker.processConsumerControl(consumerExchange, control);
        return null;
    }

    protected synchronized TransportConnectionState registerConnectionState(ConnectionId connectionId,
                                                                            TransportConnectionState state) {
        TransportConnectionState cs = null;
        //已经有一个连接状态了并且现有的注册对象不能处理多个状态对象 这个时候就会把注册对象改为map的实现
        //明明前面加了处理 一个连接只会存在一个状态对象 这里又是干什么呢
        if (!connectionStateRegister.isEmpty() && !connectionStateRegister.doesHandleMultipleConnectionStates()) {
            // swap implementations
            TransportConnectionStateRegister newRegister = new MapTransportConnectionStateRegister();
            newRegister.intialize(connectionStateRegister);
            connectionStateRegister = newRegister;
        }
        cs = connectionStateRegister.registerConnectionState(connectionId, state);
        return cs;
    }

    protected synchronized TransportConnectionState unregisterConnectionState(ConnectionId connectionId) {
        return connectionStateRegister.unregisterConnectionState(connectionId);
    }

    protected synchronized List<TransportConnectionState> listConnectionStates() {
        return connectionStateRegister.listConnectionStates();
    }

    protected synchronized TransportConnectionState lookupConnectionState(String connectionId) {
        return connectionStateRegister.lookupConnectionState(connectionId);
    }

    protected synchronized TransportConnectionState lookupConnectionState(ConsumerId id) {
        return connectionStateRegister.lookupConnectionState(id);
    }

    protected synchronized TransportConnectionState lookupConnectionState(ProducerId id) {
        return connectionStateRegister.lookupConnectionState(id);
    }

    protected synchronized TransportConnectionState lookupConnectionState(SessionId id) {
        return connectionStateRegister.lookupConnectionState(id);
    }

    // public only for testing
    public synchronized TransportConnectionState lookupConnectionState(ConnectionId connectionId) {
        return connectionStateRegister.lookupConnectionState(connectionId);
    }

    protected synchronized void setDuplexNetworkConnectorId(String duplexNetworkConnectorId) {
        this.duplexNetworkConnectorId = duplexNetworkConnectorId;
    }

    protected synchronized String getDuplexNetworkConnectorId() {
        return this.duplexNetworkConnectorId;
    }

    public boolean isStopping() {
        return stopping.get();
    }

    protected CountDownLatch getStopped() {
        return stopped;
    }

    private int getProducerCount(ConnectionId connectionId) {
        int result = 0;
        TransportConnectionState cs = lookupConnectionState(connectionId);
        if (cs != null) {
            for (SessionId sessionId : cs.getSessionIds()) {
                SessionState sessionState = cs.getSessionState(sessionId);
                if (sessionState != null) {
                    result += sessionState.getProducerIds().size();
                }
            }
        }
        return result;
    }

    private int getConsumerCount(ConnectionId connectionId) {
        int result = 0;
        TransportConnectionState cs = lookupConnectionState(connectionId);
        if (cs != null) {
            for (SessionId sessionId : cs.getSessionIds()) {
                SessionState sessionState = cs.getSessionState(sessionId);
                if (sessionState != null) {
                    result += sessionState.getConsumerIds().size();
                }
            }
        }
        return result;
    }

    public WireFormatInfo getRemoteWireFormatInfo() {
        return wireFormatInfo;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.state.CommandVisitor#processBrokerSubscriptionInfo(org.apache.activemq.command.BrokerSubscriptionInfo)
     */
    @Override
    public Response processBrokerSubscriptionInfo(BrokerSubscriptionInfo info) throws Exception {
        return null;
    }
}
