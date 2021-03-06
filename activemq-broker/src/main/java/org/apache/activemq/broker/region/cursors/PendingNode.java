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

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.util.LinkedNode;

public class PendingNode extends LinkedNode {
    private final MessageReference message;
    //存这个东西干什么
    private final OrderedPendingList list;
    public PendingNode(OrderedPendingList list,MessageReference message) {
        this.list = list;
        this.message = message;
    }

    MessageReference getMessage() {
        return this.message;
    }
    
    OrderedPendingList getList() {
        return this.list;
    }
    
    @Override
    public String toString() {
        PendingNode n = (PendingNode) getNext();
        String str = "PendingNode(";
        str += System.identityHashCode(this) + "),root="+isHeadNode()+",next="+(n != null ?System.identityHashCode(n):"NULL");
        return str;
    }

}
