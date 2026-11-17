grpc & dubbo 服务提供者客户端文件路径，客户端服务实例缓存如果开启从缓存加载实例开关，则初始化客户端服务实例缓存，则从磁盘文件加载服务实例；

```sh
DEFAULT_GROUP%40%40comosus-cherry
DEFAULT_GROUP%40%40providers%3Acom.story.home.berry.api.domain.account.service.AccountRemoteService%3A1.0.0%3A
DEFAULT_GROUP%40%40providers%3Acom.story.home.berry.api.domain.storage.StorageRemoteService%3A1.0.0%3A
DEFAULT_GROUP%40%40providers%3Acom.story.home.cherry.api.domain.storage.StorageRemoteService%3A1.0.0%3A
eptrusttry@EPdeMacBook-Pro public % cat DEFAULT_GROUP%40%40providers%3Acom.story.home.cherry.api.domain.storage.StorageRemoteService%3A1.0.0%3A
{"name":"providers:com.story.home.cherry.api.domain.storage.StorageRemoteService:1.0.0:","groupName":"DEFAULT_GROUP","clusters":"","cacheMillis":10000,"hosts":[],"lastRefTime":1793852937572,"checksum":"","allIPs":false,"reachProtectionThreshold":false,"valid":true}%                                                      
eptrusttry@EPdeMacBook-Pro public % cat DEFAULT_GROUP%40%40comosus-berry
{"name":"comosus-berry","groupName":"DEFAULT_GROUP","clusters":"","cacheMillis":10000,"hosts":[{"instanceId":"192.168.31.125#7777#DEFAULT#DEFAULT_GROUP@@comosus-berry","ip":"192.168.31.125","port":7777,"weight":1.0,"healthy":true,"enabled":true,"ephemeral":true,"clusterName":"DEFAULT","serviceName":"DEFAULT_GROUP@@comosus-berry","metadata":{"preserved.register.source":"SPRING_CLOUD","gRPC_port":"7070"},"instanceHeartBeatInterval":5000,"instanceHeartBeatTimeOut":15000,"ipDeleteTimeout":30000}],"lastRefTime":1794895517506,"checksum":"","allIPs":false,"reachProtectionThreshold":false,"valid":true}%                                     
eptrusttry@EPdeMacBook-Pro public % pwd
/Users/eptrusttry/nacos/naming/public
eptrusttry@EPdeMacBook-Pro public % 

```



```sh
eptrusttry@EPdeMacBook-Pro public % ls
DEFAULT_GROUP%40%40comosus-berry
DEFAULT_GROUP%40%40comosus-cherry
DEFAULT_GROUP%40%40providers%3Acom.story.home.berry.api.domain.account.service.AccountRemoteService%3A1.0.0%3A
DEFAULT_GROUP%40%40providers%3Acom.story.home.berry.api.domain.storage.StorageRemoteService%3A1.0.0%3A
DEFAULT_GROUP%40%40providers%3Acom.story.home.cherry.api.domain.storage.StorageRemoteService%3A1.0.0%3A
eptrusttry@EPdeMacBook-Pro public % cd ..
eptrusttry@EPdeMacBook-Pro naming % ls
dubbodev	public
eptrusttry@EPdeMacBook-Pro naming % cd dubbodev 
eptrusttry@EPdeMacBook-Pro dubbodev % ls
DEFAULT_GROUP%40%40providers%3Acom.story.home.berry.api.domain.account.service.AccountRemoteService%3A1.0.0%3A
DEFAULT_GROUP%40%40providers%3Acom.story.home.cherry.api.domain.storage.StorageRemoteService%3A1.0.0%3A
eptrusttry@EPdeMacBook-Pro dubbodev % cat DEFAULT_GROUP%40%40providers%3Acom.story.home.cherry.api.domain.storage.StorageRemoteService%3A1.0.0%3A
{"name":"providers:com.story.home.cherry.api.domain.storage.StorageRemoteService:1.0.0:","groupName":"DEFAULT_GROUP","clusters":"","cacheMillis":10000,"hosts":[{"instanceId":"192.168.31.125#20880#null#DEFAULT_GROUP@@providers:com.story.home.cherry.api.domain.storage.StorageRemoteService:1.0.0:","ip":"192.168.31.125","port":20880,"weight":1.0,"healthy":true,"enabled":true,"ephemeral":true,"clusterName":"DEFAULT","serviceName":"DEFAULT_GROUP@@providers:com.story.home.cherry.api.domain.storage.StorageRemoteService:1.0.0:","metadata":{"side":"provider","methods":"deduct","release":"2.7.8","deprecated":"false","dubbo":"2.0.2","threads":"400","pid":"93343","interface":"com.story.home.cherry.api.domain.storage.StorageRemoteService","version":"1.0.0","generic":"false","timeout":"6000000","revision":"1.0.0","retries":"0","path":"com.story.home.cherry.api.domain.storage.StorageRemoteService","protocol":"dubbo","metadata-type":"remote","application":"comosus-cherry","dynamic":"true","service.filter":"elapsedTimeFilter","category":"providers","anyhost":"true","timestamp":"1794895508441"},"instanceHeartBeatInterval":5000,"instanceHeartBeatTimeOut":15000,"ipDeleteTimeout":30000}],"lastRefTime":1794895508963,"checksum":"","allIPs":false,"reachProtectionThreshold":false,"valid":true}%                                                                       eptrusttry@EPdeMacBook-Pro dubbodev % cat DEFAULT_GROUP%40%40providers%3Acom.story.home.berry.api.domain.account.service.AccountRemoteService%3A1.0.0%3A
{"name":"providers:com.story.home.berry.api.domain.account.service.AccountRemoteService:1.0.0:","groupName":"DEFAULT_GROUP","clusters":"","cacheMillis":10000,"hosts":[{"instanceId":"192.168.31.125#30880#null#DEFAULT_GROUP@@providers:com.story.home.berry.api.domain.account.service.AccountRemoteService:1.0.0:","ip":"192.168.31.125","port":30880,"weight":1.0,"healthy":true,"enabled":true,"ephemeral":true,"clusterName":"DEFAULT","serviceName":"DEFAULT_GROUP@@providers:com.story.home.berry.api.domain.account.service.AccountRemoteService:1.0.0:","metadata":{"side":"provider","methods":"debit","release":"2.7.8","deprecated":"false","dubbo":"2.0.2","threads":"400","pid":"93354","interface":"com.story.home.berry.api.domain.account.service.AccountRemoteService","version":"1.0.0","generic":"false","timeout":"6000000","revision":"1.0.0","retries":"0","path":"com.story.home.berry.api.domain.account.service.AccountRemoteService","protocol":"dubbo","metadata-type":"remote","application":"comosus-berry","dynamic":"true","service.filter":"elapsedTimeFilter","category":"providers","anyhost":"true","timestamp":"1794895516572"},"instanceHeartBeatInterval":5000,"instanceHeartBeatTimeOut":15000,"ipDeleteTimeout":30000}],"lastRefTime":1794895516890,"checksum":"","allIPs":false,"reachProtectionThreshold":false,"valid":true}%                                      eptrusttry@EPdeMacBook-Pro dubbodev % 

```