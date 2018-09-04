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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.command.ConnectionInfo;

/**
 * 
 */

public class TransportConnectionState extends org.apache.activemq.state.ConnectionState {

    private ConnectionContext context;
    //很多个具体的连接都对应一个这个对象
    private TransportConnection connection;
    //每次调用addConnection时才增加 感觉像是表示一个连接被建立的次数
    //应该是客户端和broker端断开了 然后重连的时候并没有重新非配一个连接id
    private AtomicInteger referenceCounter = new AtomicInteger();
    //大神们很喜欢新建个Object对象当lock使用啊
    //相对于直接用this有什么优点吗
    private final Object connectionMutex = new Object();

    public TransportConnectionState(ConnectionInfo info, TransportConnection transportConnection) {
        super(info);
        connection = transportConnection;
    }

    public ConnectionContext getContext() {
        return context;
    }

    public TransportConnection getConnection() {
        return connection;
    }

    public void setContext(ConnectionContext context) {
        this.context = context;
    }

    public void setConnection(TransportConnection connection) {
        this.connection = connection;
    }

    public int incrementReference() {
        return referenceCounter.incrementAndGet();
    }

    public int decrementReference() {
        return referenceCounter.decrementAndGet();
    }

	public AtomicInteger getReferenceCounter() {
		return referenceCounter;
	}

	public void setReferenceCounter(AtomicInteger referenceCounter) {
		this.referenceCounter = referenceCounter;
	}

	public Object getConnectionMutex() {
		return connectionMutex;
	}
}
