
# GRPC连接建立处理器

com.alibaba.nacos.core.remote.grpc.GrpcBiStreamRequestAcceptor.requestBiStream





# GRPC请求处理器
com.alibaba.nacos.core.remote.grpc.GrpcRequestAcceptor.request
com.alibaba.nacos.core.remote.RequestHandlerRegistry.getByRequestType
com.alibaba.nacos.core.remote.RequestHandlerRegistry.onApplicationEvent
包含各类GPRC处理器


qcode 总结RequestHandler的类型及作用

## 
com.alibaba.nacos.config.server.remote.ConfigChangeBatchListenRequestHandler

##

com.alibaba.nacos.config.server.remote.ConfigPublishRequestHandler


## 
com.alibaba.nacos.config.server.remote.ConfigRemoveRequestHandler

## 
com.alibaba.nacos.naming.remote.rpc.handler.SubscribeServiceRequestHandler


## 
com.alibaba.nacos.config.server.remote.ConfigQueryRequestHandler

## 

com.alibaba.nacos.naming.remote.rpc.handler.ServiceQueryRequestHandler

## 
com.alibaba.nacos.core.cluster.remote.MemberReportHandler

## 
com.alibaba.nacos.core.remote.HealthCheckRequestHandler