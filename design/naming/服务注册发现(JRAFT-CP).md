# 服务实例注册与注销

## 客户端
发起服务发起服务请求
com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy.doRegisterServiceForPersistent
com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy.doDeregisterServiceForPersistent


PersistentInstanceRequest



## 服务端
[//]: # (TODO)
com.alibaba.nacos.naming.remote.rpc.handler.PersistentInstanceRequestHandler
com.alibaba.nacos.naming.remote.rpc.handler.PersistentInstanceRequestHandler.registerInstance


### 实例注册
持久化注册服务实例的方式，除了有Jraft协议集群处理注册写请求，触发NacosStateMachine状态机，处理服务注册，后续逻辑与正常服务实例注册一致

PersistentInstanceRequest
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.registerInstance
InstanceStoreRequest
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.write
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.writeAsync
com.alibaba.nacos.core.distributed.raft.JRaftServer.commit
com.alibaba.nacos.core.distributed.raft.JRaftServer.findTupleByGroup


leader模式
com.alibaba.nacos.core.distributed.raft.JRaftServer.applyOperation


com.alipay.sofa.jraft.core.NodeImpl.apply
**node.apply(task) → JRaft 内部：写本地 log → 复制到 follower → 多数派提交 → StateMachine.onApply**

委托处理器处理写请求
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onApply




处理注册链路
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.addRequestProcessors
com.alibaba.nacos.core.distributed.raft.JRaftServer.start
com.alibaba.nacos.core.distributed.raft.JRaftServer.createMultiRaftGroup

com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.onApply
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.onInstanceRegister

后续链路与正常的服务实例注册一致


转发给leader, 剩余与leader模式执行一致；
com.alibaba.nacos.core.distributed.raft.JRaftServer.invokeToLeader

com.alibaba.nacos.core.distributed.raft.JRaftServer.start
com.alibaba.nacos.core.distributed.raft.utils.JRaftUtils.initRpcServer
com.alibaba.nacos.core.distributed.raft.processor.NacosWriteRequestProcessor.handleRequest
com.alibaba.nacos.core.distributed.raft.processor.AbstractProcessor.handleRequest


### 实例注销

com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.deregisterInstance
与服务实例注销流程基本一致，只不过操作类型为：DataOperation.DELETE


com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.onApply
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.onInstanceRegister
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.onInstanceDeregister

后续链路与正常的服务实例注销一致


# 服务订阅与取消订阅
目前服务订阅与取消订阅，不支持JRAFT方式；订阅和取消订阅，与临时服务注册注销的服务端的订阅和取消订阅基本一致；需要验证

## 客户端
com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy.subscribe
com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy.unsubscribe
SubscribeServiceRequest

## 服务端

SubscribeServiceRequest
com.alibaba.nacos.naming.remote.rpc.handler.SubscribeServiceRequestHandler.handle



