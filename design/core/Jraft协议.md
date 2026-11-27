
# Jraft相关配置

```
#*************** JRaft Related Configurations ***************#

### Sets the Raft cluster election timeout, default value is 5 second
# nacos.core.protocol.raft.data.election_timeout_ms=5000
### Sets the amount of time the Raft snapshot will execute periodically, default is 30 minute
# nacos.core.protocol.raft.data.snapshot_interval_secs=30
### raft internal worker threads
# nacos.core.protocol.raft.data.core_thread_num=8
### Number of threads required for raft business request processing
# nacos.core.protocol.raft.data.cli_service_thread_num=4
### raft linear read strategy. Safe linear reads are used by default, that is, the Leader tenure is confirmed by heartbeat
# nacos.core.protocol.raft.data.read_index_type=ReadOnlySafe
### rpc request timeout, default 5 seconds
# nacos.core.protocol.raft.data.rpc_request_timeout_ms=5000
```


com.alibaba.nacos.core.distributed.ConsistencyConfiguration.strongAgreementProtocol

com.alibaba.nacos.core.distributed.ProtocolManager
* JRaftProtocol
* JRaftServer
* NacosStateMachine

* SwitchManager
  BatchWriteRequest
  com.alibaba.nacos.naming.misc.SwitchManager.onApply


# JRaftProtocol


com.alibaba.nacos.core.distributed.ProtocolManager.getCpProtocol
com.alibaba.nacos.core.distributed.ProtocolManager.initCPProtocol

初始化raft集群成员信息
com.alibaba.nacos.core.distributed.ProtocolManager.injectMembers4CP

监听raft集群事件：RaftEvent
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.init

# 刷新节点状态

节点启动后会启动刷新节点状态，如果raft group leader已经产生，则会立即刷新节点状态为UP

com.alibaba.nacos.naming.cluster.ServerStatusManager.init
ServerStatusUpdater
com.alibaba.nacos.naming.cluster.ServerStatusManager.refreshServerStatus
com.alibaba.nacos.naming.cluster.ServerStatusManager.isReady

com.alibaba.nacos.core.distributed.raft.JRaftProtocol.isReady
com.alibaba.nacos.core.distributed.raft.JRaftServer.isReady
com.alibaba.nacos.core.distributed.raft.JRaftServer.getLeader
com.alipay.sofa.jraft.RouteTable.selectLeader


## 当前节点是否为leader
判断是不是leader都是基于底层的Raft机制，判断节点是否是leader以及是否ready
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.isLeader
com.alibaba.nacos.core.distributed.raft.JRaftServer.findNodeByGroup
com.alipay.sofa.jraft.core.NodeImpl.isLeader()

## 协议是否ready
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.isReady
com.alibaba.nacos.core.distributed.raft.JRaftServer.isReady
com.alibaba.nacos.core.distributed.raft.JRaftServer.getLeader
com.alipay.sofa.jraft.RouteTable.selectLeader


## raft leader事件
成为leader或follower时，会抛出RaftEvent，JRaftProtocol更新raft集群leader状态，这是EmbeddedDumpService就可以执行相关的dump操作
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onLeaderStart
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onStartFollowing


RaftEvent


com.alibaba.nacos.core.distributed.raft.JRaftProtocol.init
com.alibaba.nacos.config.server.service.dump.EmbeddedDumpService.init




## 集群状态机配置变更
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onConfigurationCommitted

## 集群状态机错误处理
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onError

## leader 停止
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onLeaderStop

## 集群节点变更

**当前节点变更只支持移除节点操作，扩容节点当前暂不支持**

[Jraft Leader选举及成员变更](Jraft Leader选举及成员变更.md)   

com.alibaba.nacos.core.distributed.ProtocolManager.onEvent
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.memberChange
com.alibaba.nacos.core.distributed.raft.JRaftServer.peerChange
com.alibaba.nacos.core.distributed.raft.JRaftMaintainService.execute(java.util.Map<java.lang.String,java.lang.String>)
com.alibaba.nacos.core.distributed.raft.JRaftServer.findNodeByGroup
com.alibaba.nacos.core.distributed.raft.JRaftMaintainService.single


###  集群成员节点状态
com.alibaba.nacos.naming.core.DistroMapper.onEvent



# JRaftServer
## 初始化JRaftServer
com.alibaba.nacos.core.distributed.raft.JRaftServer.init
初始化raft执行器
com.alibaba.nacos.core.distributed.raft.utils.RaftExecutor.init

创建raft客户端服务
com.alipay.sofa.jraft.RaftServiceFactory.createAndInitCliService

## 启动JRaftServer
com.alibaba.nacos.core.distributed.raft.JRaftServer.start
添加集群节点到Raft节点管理器
com.alipay.sofa.jraft.NodeManager.addAddress

初始化Rpc服务器
com.alibaba.nacos.core.distributed.raft.utils.JRaftUtils.initRpcServer

### 添加raftGroup multiRaftGroup
com.alibaba.nacos.core.distributed.raft.JRaftServer.createMultiRaftGroup

初始化log，快照，元数据目录
com.alibaba.nacos.core.distributed.raft.utils.JRaftUtils.initDirectory
初始化快照保存间隔，状态机NacosStateMachine
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.NacosStateMachine

创建raft组服务
com.alipay.sofa.jraft.RaftGroupService
com.alipay.sofa.jraft.RaftGroupService.start(boolean)
com.alipay.sofa.jraft.RaftServiceFactory.createRaftNode
com.alipay.sofa.jraft.core.NodeImpl.NodeImpl(java.lang.String, com.alipay.sofa.jraft.entity.PeerId)
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.setNode
com.alipay.sofa.jraft.RouteTable.updateConfiguration(java.lang.String, com.alipay.sofa.jraft.conf.Configuration)
注册节点自己到raft簇
com.alibaba.nacos.core.distributed.raft.JRaftServer.registerSelfToCluster
com.alipay.sofa.jraft.core.CliServiceImpl.addPeer
定时刷新路由表
com.alibaba.nacos.core.distributed.raft.JRaftServer.refreshRouteTable

#### 添加请求处理器
每个处理器有自己的group

com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.init
com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.group


com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadataProcessor.InstanceMetadataProcessor
com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadataProcessor.ServiceMetadataProcessor
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.PersistentClientOperationServiceImpl


### JRaft写请求
leader直接写，否则转发给follower
com.alibaba.nacos.core.distributed.raft.JRaftProtocol.write
com.alibaba.nacos.core.distributed.raft.JRaftServer.commit
com.alibaba.nacos.core.distributed.raft.JRaftServer.applyOperation
com.alibaba.nacos.core.distributed.raft.JRaftServer.invokeToLeader

com.alipay.sofa.jraft.core.NodeImpl.apply
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onApply

写请求处理具体有如下几种
com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.onApply
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.onApply
com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadataProcessor.onApply
com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadataProcessor.onApply



#### 写请求处理器
NacosWriteRequestProcessor
针对写请求，如果是leader，则直接执行写请求，否则相应无法发现Leader
com.alibaba.nacos.core.distributed.raft.processor.NacosWriteRequestProcessor.handleRequest
com.alibaba.nacos.core.distributed.raft.processor.AbstractProcessor.handleRequest
com.alibaba.nacos.core.distributed.raft.processor.AbstractProcessor.execute
com.alibaba.nacos.core.distributed.raft.JRaftServer.applyOperation
com.alipay.sofa.jraft.core.NodeImpl.apply


### Raft读请求
leader直接写，否则转发给follower

com.alibaba.nacos.core.distributed.raft.JRaftServer.get
com.alibaba.nacos.core.distributed.raft.JRaftServer.readFromLeader
com.alibaba.nacos.core.distributed.raft.JRaftServer.commit
com.alibaba.nacos.core.distributed.raft.JRaftServer.applyOperation
com.alibaba.nacos.core.distributed.raft.JRaftServer.invokeToLeader

com.alipay.sofa.jraft.core.NodeImpl.apply
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onApply


写请求处理具体有如下几种
直接读derby数据库，与dump的时候，直接读leader，leader的读，实际是读底层的derby数据库；
com.alibaba.nacos.core.persistence.DistributedDatabaseOperateImpl.onRequest

其他几个处理器都不知道读
com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadataProcessor.onRequest
com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadataProcessor.onRequest
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.onRequest


#### raft读请求处理器

针对读请求，如果是leader，则直接执行读请求，否则相应无法发现Leader

com.alibaba.nacos.core.distributed.raft.processor.NacosReadRequestProcessor.handleRequest
com.alibaba.nacos.core.distributed.raft.processor.AbstractProcessor.handleRequest
com.alibaba.nacos.core.distributed.raft.processor.AbstractProcessor.execute
com.alibaba.nacos.core.distributed.raft.JRaftServer.applyOperation
com.alibaba.nacos.core.distributed.raft.JRaftServer.invokeToLeader

com.alipay.sofa.jraft.core.NodeImpl.apply
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onApply






# 集群统一开关控制
[SwitchManager作用](/design/naming/SwitchManager作用.md)
com.alibaba.nacos.naming.misc.SwitchManager.onApply
com.alibaba.nacos.naming.misc.SwitchManager.update(com.alibaba.nacos.naming.misc.SwitchDomain)
com.alibaba.nacos.naming.misc.SwitchManager.onRequest
