/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.liyue2008.rpc.serialize;

import com.github.liyue2008.rpc.spi.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 首先我们需要设计一个可扩展的，通用的序列化接口，为了方便使用，
 * 我们直接使用静态类的方式来定义这个接口（严格来说这并不是一个接口）
 *
 * @author LiYue
 * Date: 2019/9/20
 */
@SuppressWarnings("unchecked")
public class SerializeSupport {
    private static final Logger logger = LoggerFactory.getLogger(SerializeSupport.class);

    /**
     * 它的用途是在序列化的时候，通过被序列化的对象类型，找到对应的序列化实现类
     */
    private static Map<Class<?>/*序列化对象类型*/, Serializer<?>/*序列化实现*/> serializerMap = new HashMap<>();

    /**
     * 用于在反序列化的时候，从序列化的数据中读出对象类型，然后找到对应的序列化实现类
     */
    private static Map<Byte/*序列化实现类型*/, Class<?>/*序列化对象类型*/> typeMap = new HashMap<>();

    static {
        for (Serializer serializer : ServiceSupport.loadAll(Serializer.class)) {
            registerType(serializer.type(), serializer.getSerializeClass(), serializer);
            logger.info("Found serializer, class: {}, type: {}.",
                    serializer.getSerializeClass().getCanonicalName(),
                    serializer.type());
        }
    }

    private static byte parseEntryType(byte[] buffer) {
        return buffer[0];
    }

    private static <E> void registerType(byte type, Class<E> eClass, Serializer<E> serializer) {
        serializerMap.put(eClass, serializer);
        typeMap.put(type, eClass);
    }

    @SuppressWarnings("unchecked")
    private static <E> E parse(byte[] buffer, int offset, int length, Class<E> eClass) {
        Object entry = serializerMap.get(eClass).parse(buffer, offset, length);
        // todo isAssignableFrom？
        if (eClass.isAssignableFrom(entry.getClass())) {
            return (E) entry;
        } else {
            throw new SerializeException("Type mismatch!");
        }
    }

    /**
     * 反序列化
     *
     * @param buffer
     * @param <E>
     * @return
     */
    public static <E> E parse(byte[] buffer) {
        return parse(buffer, 0, buffer.length);
    }

    private static <E> E parse(byte[] buffer, int offset, int length) {
        byte type = parseEntryType(buffer);
        // 从序列化的数据中读出对象类型，然后找到对应的序列化实现类
        @SuppressWarnings("unchecked")
        Class<E> eClass = (Class<E>) typeMap.get(type);
        if (null == eClass) {
            throw new SerializeException(String.format("Unknown entry type: %d!", type));
        } else {
            return parse(buffer, offset + 1, length - 1, eClass);
        }

    }

    /**
     * 序列化
     *
     * @param entry
     * @param <E>
     * @return
     */
    public static <E> byte[] serialize(E entry) {
        // 类型转换为具体的序列化实现类（都实现了 Serializer 接口）
        @SuppressWarnings("unchecked")
        Serializer<E> serializer = (Serializer<E>) serializerMap.get(entry.getClass());
        if (serializer == null) {
            throw new SerializeException(String.format("Unknown entry class type: %s", entry.getClass().toString()));
        }
        // 扩展长度，头部用户存放类型编码
        byte[] bytes = new byte[serializer.size(entry) + 1];
        // bytes[0] 放序列化对象的类型，实际的数据从 offset=1 开始序列化
        bytes[0] = serializer.type();
        // 调用具体实现类的的序列化方法
        serializer.serialize(entry, bytes, 1, bytes.length - 1);
        return bytes;
    }
}
