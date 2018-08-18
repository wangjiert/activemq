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

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Holder for many bitArrays - used for message audit
 *
 *
 */
public class BitArrayBin implements Serializable {

    private static final long serialVersionUID = 1L;
    private final LinkedList<BitArray> list;
    //记录了这个array有多少个bitarray
    private int maxNumberOfArrays;
    private int firstIndex = -1;  // leave 'int' for old serialization compatibility and introduce new 'long' field
    private long lastInOrderBit=-1;
    //这个是每次新加东西都会加吗
    //这应该是表示序列是从什么位置开始的吧
    private long longFirstIndex=-1;
    /**
     * Create a BitArrayBin to a certain window size (number of messages to
     * keep)
     *
     * @param windowSize
     */
    public BitArrayBin(int windowSize) {
        //果然多申请了空间
        maxNumberOfArrays = ((windowSize + 1) / BitArray.LONG_SIZE) + 1;
        maxNumberOfArrays = Math.max(maxNumberOfArrays, 1);
        list = new LinkedList<BitArray>();
        for (int i = 0; i < maxNumberOfArrays; i++) {
            list.add(null);
        }
    }

    /**
     * Set a bit
     *
     * @param index
     * @param value
     * @return true if set
     */
    public boolean setBit(long index, boolean value) {
        boolean answer = false;
        BitArray ba = getBitArray(index);
        if (ba != null) {
            int offset = getOffset(index);
            if (offset >= 0) {
                answer = ba.set(offset, value);
            }
        }
        return answer;
    }

    /**
     * Test if in order
     * @param index
     * @return true if next message is in order
     */
    public boolean isInOrder(long index) {
        boolean result = false;
        if (lastInOrderBit == -1) {
            result = true;
        } else {
            result = lastInOrderBit + 1 == index;
        }
        lastInOrderBit = index;
        return result;

    }

    /**
     * Get the boolean value at the index
     *
     * @param index
     * @return true/false
     */
    public boolean getBit(long index) {
        boolean answer = index >= longFirstIndex;
        BitArray ba = getBitArray(index);
        if (ba != null) {
            int offset = getOffset(index);
            if (offset >= 0) {
                answer = ba.get(offset);
                return answer;
            }
        } else {
            // gone passed range for previous bins so assume set
            answer = true;
        }
        return answer;
    }

    /**
     * Get the BitArray for the index
     *
     * @param index
     * @return BitArray
     */
    //这个里面应该就是longFirstIndex赋值的地方
    private BitArray getBitArray(long index) {
        int bin = getBin(index);
        BitArray answer = null;
        if (bin >= 0) {
            if (bin >= maxNumberOfArrays) {
                int overShoot = bin - maxNumberOfArrays + 1;
                while (overShoot > 0) {
                    list.removeFirst();
                    longFirstIndex += BitArray.LONG_SIZE;
                    list.add(new BitArray());
                    overShoot--;
                }

                bin = maxNumberOfArrays - 1;
            }
            answer = list.get(bin);
            if (answer == null) {
                answer = new BitArray();
                list.set(bin, answer);
            }
        }
        return answer;
    }

    /**
     * Get the index of the bin from the total index
     *
     * @param index
     * @return the index of the bin
     */
    private int getBin(long index) {
        int answer = 0;
        if (longFirstIndex < 0) {
            //这个index为什么不加一呢
            longFirstIndex = (index - (index % BitArray.LONG_SIZE));
        } else if (longFirstIndex >= 0) {
            answer = (int)((index - longFirstIndex) / BitArray.LONG_SIZE);
        }
        return answer;
    }

    /**
     * Get the offset into a bin from the total index
     *
     * @param index
     * @return the relative offset into a bin
     */
    private int getOffset(long index) {
        int answer = 0;
        if (longFirstIndex >= 0) {
            answer = (int)((index - longFirstIndex) - (BitArray.LONG_SIZE * getBin(index)));
        }
        return answer;
    }

    public long getLastSetIndex() {
        long result = -1;

        if (longFirstIndex >=0) {
            result = longFirstIndex;
            BitArray last = null;
            for (int lastBitArrayIndex = maxNumberOfArrays -1; lastBitArrayIndex >= 0; lastBitArrayIndex--) {
                last = list.get(lastBitArrayIndex);
                //这里还进行了是否为空的判断说明预申请的数量是大于实际值的
                if (last != null) {
                    //这里减1是不是说明返回的序列的计算是从0开始的
                    result += last.length() -1;
                    //为什么要乘以64
                    //计算思路就是 这个数组里面有n个bitarray说明前n-1个bitarray是满的 而这个对象里面记录了多少个对应位就说明发了多少条消息
                    //所以用n-1乘以64加上最后一个bitarray的记录数就是总共的消息数
                    result += lastBitArrayIndex * BitArray.LONG_SIZE;
                    break;
                }
            }
        }
        return result;
    }
}
