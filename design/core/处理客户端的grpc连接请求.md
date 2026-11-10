
# GRPC连接建立处理器

处理客户端的连接，添加连接到连接管理器
com.alibaba.nacos.core.remote.grpc.GrpcBiStreamRequestAcceptor.requestBiStream
com.alibaba.nacos.core.remote.ConnectionManager.register
连接属性connetionId， port，address配置
com.alibaba.nacos.core.remote.grpc.AddressTransportFilter.transportReady




# GRPC请求处理器
com.alibaba.nacos.core.remote.grpc.GrpcRequestAcceptor.request
com.alibaba.nacos.core.remote.RequestHandlerRegistry.getByRequestType
com.alibaba.nacos.core.remote.RequestHandlerRegistry.onApplicationEvent
包含各类GPRC处理器


qcode 总结RequestHandler的类型及作用

## 批量请求配置变更处理器

处理客户端配置的监听和注销，如果配置是灰度控制，则进行灰度匹配
com.alibaba.nacos.config.server.remote.ConfigChangeBatchListenRequestHandler
com.alibaba.nacos.config.server.service.ConfigCacheService.isUptodate(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.util.Map<java.lang.String,java.lang.String>)
com.alibaba.nacos.config.server.service.ConfigCacheService.getContentMd5(java.lang.String, java.lang.String, java.lang.String, java.util.Map<java.lang.String,java.lang.String>)
com.alibaba.nacos.config.server.model.ConfigCacheGray.match
com.alibaba.nacos.config.server.model.gray.BetaGrayRule.match
com.alibaba.nacos.config.server.model.gray.TagGrayRule.match

## 配置查询请求处理器
com.alibaba.nacos.config.server.remote.ConfigQueryRequestHandler

## 配置发布请求处理器

com.alibaba.nacos.config.server.remote.ConfigPublishRequestHandler

## 配置移除请求处理器
com.alibaba.nacos.config.server.remote.ConfigRemoveRequestHandler


## 健康检查请求处理器
com.alibaba.nacos.core.remote.HealthCheckRequestHandler

## 订阅服务请求处理器
com.alibaba.nacos.naming.remote.rpc.handler.SubscribeServiceRequestHandler

## 服务查询请求处理器

com.alibaba.nacos.naming.remote.rpc.handler.ServiceQueryRequestHandler

## 会员报告处理器
com.alibaba.nacos.core.cluster.remote.MemberReportHandler

