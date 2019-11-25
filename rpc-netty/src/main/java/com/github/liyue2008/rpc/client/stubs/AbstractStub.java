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
package com.github.liyue2008.rpc.client.stubs;

import com.github.liyue2008.rpc.client.RequestIdSupport;
import com.github.liyue2008.rpc.client.ServiceStub;
import com.github.liyue2008.rpc.client.ServiceTypes;
import com.github.liyue2008.rpc.serialize.SerializeSupport;
import com.github.liyue2008.rpc.transport.Transport;
import com.github.liyue2008.rpc.transport.command.Code;
import com.github.liyue2008.rpc.transport.command.Command;
import com.github.liyue2008.rpc.transport.command.Header;
import com.github.liyue2008.rpc.transport.command.ResponseHeader;

import java.util.concurrent.ExecutionException;

/**
 * AbstractStub 的抽象类，在这个类中实现大部分通用的逻辑，让所有动态生成的桩都继承这个抽象类，
 * 这样动态生成桩的代码会更少一些
 *
 * @author LiYue
 * Date: 2019/9/27
 */
public abstract class AbstractStub implements ServiceStub {
    protected Transport transport;

    protected byte[] invokeRemote(RpcRequest request) {
        Header header = new Header(ServiceTypes.TYPE_RPC_REQUEST, 1, RequestIdSupport.next());
        byte[] payload = SerializeSupport.serialize(request);
        Command requestCommand = new Command(header, payload);
        try {
            Command responseCommand = transport.send(requestCommand).get();
            ResponseHeader responseHeader = (ResponseHeader) responseCommand.getHeader();
            if (responseHeader.getCode() == Code.SUCCESS.getCode()) {
                return responseCommand.getPayload();
            } else {
                throw new Exception(responseHeader.getError());
            }

        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setTransport(Transport transport) {
        this.transport = transport;
    }
}
