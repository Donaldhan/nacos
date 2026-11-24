状态机定时快照及加载快照的时机参加

[Jraft状态机的快照保存与加载时机](/design/core/Jraft状态机的快照保存与加载时机.md) 

具体链路参见

[配置发布Jraft](/design/config/配置发布Jraft.md)

1. 定时快照数据

[//]: # (TODO)
本节点 snapshotTimer 定时 1800s，首次随机化到 900~1350s
com.alipay.sofa.jraft.core.NodeImpl.init
com.alipay.sofa.jraft.core.NodeImpl.handleSnapshotTimeout
com.alipay.sofa.jraft.core.NodeImpl.doSnapshot
com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl.doSnapshot(com.alipay.sofa.jraft.Closure, boolean)
com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl.doSnapshot(com.alipay.sofa.jraft.Closure, boolean)
com.alipay.sofa.jraft.core.FSMCallerImpl.onSnapshotSaveSync(com.alipay.sofa.jraft.closure.SaveSnapshotClosure)
com.alipay.sofa.jraft.core.FSMCallerImpl.onSnapshotSaveSync(com.alipay.sofa.jraft.core.FSMCallerImpl.ApplyTask)
com.alipay.sofa.jraft.core.FSMCallerImpl.doSnapshotSave
com.alipay.sofa.jraft.core.StateMachineAdapter.onSnapshotSave


com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onSnapshotSave
com.alibaba.nacos.core.persistence.DerbySnapshotOperation.onSnapshotSave


2. 启动是从快照加载

[//]: # (TODO)
onSnapshotLoad 节点启动加载本地最近快照
SnapshotExecutorImpl.init() 里 snapshotStorage.open() 拿到快照
com.alipay.sofa.jraft.core.NodeImpl.init
com.alipay.sofa.jraft.core.NodeImpl.initSnapshotStorage
com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl.init
com.alipay.sofa.jraft.core.FSMCallerImpl.onSnapshotLoad
com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onSnapshotLoad
com.alibaba.nacos.core.persistence.DerbySnapshotOperation.onSnapshotLoad



[//]: # (TODO)
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.loadSnapshotOperate
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.PersistentClientOperationServiceImpl


# 
[//]: # (TODO)
com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadataProcessor.InstanceMetadataProcessor
com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadataProcessor.loadSnapshotOperate




* com.alibaba.nacos.core.distributed.raft.NacosStateMachine.onApply， 处理器处理读写请求
* 状态只能接受写请求，不同请求有不同的处理器，如何映射起来，比如MetadataOperation<InstanceMetadata>映射的为InstanceMetadataProcessor，MetadataOperation<ServiceMetadata>>映射的为ServiceMetadataProcessor

<!-- ####  -->
com.alibaba.nacos.naming.core.v2.service.impl.PersistentClientOperationServiceImpl.onApply

<!-- #### Jraft机制下，服务实例的元数据没有，这些是不是没有用 -->


# 

[//]: # (TODO)
com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadataProcessor.ServiceMetadataProcessor
com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadataProcessor.loadSnapshotOperate


更新服务实例元数据
MetadataOperation<InstanceMetadata>
com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadataProcessor.onApply
com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadataProcessor.updateInstanceMetadata
com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager.updateInstanceMetadata

<!-- ####  Jraft机制下，服务实例的元数据没有，这些是不是没有用 - -->
MetadataOperation<ServiceMetadata>
com.alibaba.nacos.naming.core.v2.metadata.ServiceMetadataProcessor.onApply


# 

[//]: # (TODO)
com.alibaba.nacos.naming.misc.SwitchManager.SwitchManager
com.alibaba.nacos.naming.misc.SwitchManager.loadSnapshotOperate