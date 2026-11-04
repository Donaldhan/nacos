让我先读取这个设计文档，看看官方的解释。

这个文档内容是我之前给你的解析，但没有回答你的核心问题。让我搜索一下 Nacos 为什么不使用 Spring Boot 原生机制的原因。

好的，现在我明白你的问题了。让我为你详细解答：

## 🤔 为什么不使用 Spring Boot 的 @RestController 原生机制？

Nacos 自定义 `ControllerMethodsCache` 而不依赖 Spring Boot 原生机制，主要有以下几个**核心原因**：

---

## 1️⃣ **需要在请求到达 Controller 之前进行权限控制**

### 问题场景
Spring Boot 的 `HandlerMapping` 机制是在 **DispatcherServlet 内部**工作的，此时请求已经到达了 Spring MVC 层。但 Nacos 需要在**更早期**的阶段（Servlet Filter 层）进行权限判断。

### Nacos 的解决方案
```java
// AuthFilter.java - 在 Servlet Filter 层工作
public class AuthFilter implements Filter {
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) {
        HttpServletRequest req = (HttpServletRequest) request;
        
        // 🔑 关键：在请求到达 Controller 之前就判断权限
        Method method = methodsCache.getMethod(req);  // ① 获取方法
        
        if (method.isAnnotationPresent(Secured.class)) {  // ② 检查注解
            // 执行认证逻辑
            boolean result = protocolAuthService.validateIdentity(...);
            if (!result) {
                resp.sendError(HttpServletResponse.SC_FORBIDDEN);  // ③ 拒绝访问
                return;
            }
        }
        
        chain.doFilter(request, response);  // ④ 认证通过，继续执行
    }
}
```

### 如果用 Spring Boot 原生机制
```java
// ❌ 问题：此时请求已经到达 Controller 层，太晚了
@PreAuthorize("hasPermission('read')")  // Spring Security 方式
@GetMapping("/v1/ns/services")
public Result getServices() {
    // 业务逻辑
}
```

**缺点**：
- Spring Security 的 `@PreAuthorize` 基于 AOP 代理，需要在方法执行前拦截
- 但 Nacos 的权限模型更复杂，需要基于资源（Resource）、动作（Action）等细粒度控制
- 在 Filter 层拦截更高效，避免不必要的 Controller 初始化开销

---

## 2️⃣ **跨模块的统一权限控制需求**

### Nacos 的架构特点
Nacos 有多个功能模块：
- `naming`（服务发现）
- `config`（配置管理）
- `console`（控制台）
- `core`（核心功能）

每个模块都有自己的 Controller，但需要**统一的权限控制入口**。

### 自定义 ControllerMethodsCache 的优势
```java
// ConsoleConfig.java - 启动时扫描所有模块的 Controller
@PostConstruct
public void init() {
    methodsCache.initClassMethod("com.alibaba.nacos.core.controller");
    methodsCache.initClassMethod("com.alibaba.nacos.naming.controllers");
    methodsCache.initClassMethod("com.alibaba.nacos.config.server.controller");
    methodsCache.initClassMethod("com.alibaba.nacos.console.controller");
}
```

**好处**：
- ✅ 集中管理所有 Controller 的路由信息
- ✅ 统一的 Filter 入口，便于扩展
- ✅ 跨模块的权限策略一致性

### 如果用 Spring Boot 原生机制
- 每个模块需要单独配置 Spring Security 规则
- 难以实现统一的资源级权限控制
- 需要深度定制 Spring Security，复杂度高

---

## 3️⃣ **性能优化：避免重复的路由查找**

### Spring Boot 的路由查找流程
```
每次请求 → DispatcherServlet → HandlerMapping → 遍历所有 Handler → 匹配
```
- 每次请求都要执行路由匹配逻辑
- 对于大量 API 的场景，性能开销较大

### Nacos 的优化方案
```java
// 启动时预缓存
methodsCache.initClassMethod(packageName);  // 扫描并缓存

// 运行时 O(1) 查找
Method method = methodsCache.getMethod(req);  // 直接获取
```

**性能对比**：
- Spring Boot：每次请求 O(n) 查找（n = Handler 数量）
- Nacos：启动时 O(n) 预处理，运行时 O(1) 查找

---

## 4️⃣ **自定义参数校验和 TPS 控制**

除了权限控制，Nacos 还需要在 Filter 层做：

### 参数校验
```java
// ParamCheckerFilter.java
public class ParamCheckerFilter extends OncePerRequestFilter {
    @Override
    protected void doFilterInternal(HttpServletRequest request, ...) {
        Method method = controllerMethodsCache.getMethod(request);
        // 根据方法上的注解进行参数校验
        checker.check(request, method);
        chain.doFilter(request, response);
    }
}
```

### TPS 限流
```java
// NacosHttpTpsFilter.java
public class NacosHttpTpsFilter extends OncePerRequestFilter {
    @Override
    protected void doFilterInternal(HttpServletRequest request, ...) {
        Method method = controllerMethodsCache.getMethod(request);
        // 根据方法进行 TPS 控制
        tpsControl.check(method, request);
        chain.doFilter(request, response);
    }
}
```

**这些都需要在请求到达 Controller 之前获取方法信息**，Spring Boot 原生机制无法满足。

---

## 5️⃣ **历史原因和架构演进**

### 时间背景
`ControllerMethodsCache` 从 **1.2.0 版本**（2019年）就引入了，当时：
- Spring Boot 的权限控制生态还不够成熟
- Spring Security 5.x 的响应式编程模型复杂度高
- Nacos 需要更轻量级的权限控制方案

### 架构选择
```java
// 轻量级自定义注解
@Secured(resource = "service", action = ActionTypes.READ)
@GetMapping("/v1/ns/service/list")
public Result listServices() { ... }

// vs Spring Security 的复杂配置
@Configuration
@EnableWebSecurity
public class SecurityConfig extends WebSecurityConfigurerAdapter {
    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.authorizeRequests()
            .antMatchers("/v1/ns/service/**").hasRole("READ")
            // ... 大量配置
    }
}
```

**Nacos 的选择更简单直接**，符合中间件项目的定位。

---

## 📊 对比总结

| 特性 | Spring Boot 原生机制 | Nacos ControllerMethodsCache |
|------|---------------------|------------------------------|
| **拦截时机** | Controller 层（较晚） | Servlet Filter 层（早期） |
| **权限模型** | 基于角色/权限字符串 | 基于资源+动作的细粒度控制 |
| **跨模块统一管理** | 需要复杂配置 | 天然支持 |
| **性能** | 每次请求查找 | 预缓存 O(1) 查找 |
| **扩展性** | 依赖框架特性 | 完全自主可控 |
| **学习成本** | 需要深入理解 Spring Security | 简单的注解+Filter |
| **参数校验** | Controller 层 | Filter 层（更早） |
| **TPS 控制** | 需要额外集成 | 原生支持 |

---

## 🎯 核心原因总结

Nacos 不使用 Spring Boot 原生机制的**根本原因**：

1. **更早的拦截点**：在 Servlet Filter 层就能判断权限，避免无效请求到达 Controller
2. **统一的控制入口**：一个 Filter 处理所有模块的权限、参数校验、限流
3. **定制化需求**：Nacos 的资源级权限模型与 Spring Security 不兼容
4. **性能考量**：预缓存路由信息，减少每次请求的查找开销
5. **架构简洁**：避免引入 Spring Security 的复杂性

这是一个**典型的"重复造轮子"但合理的案例**——在特定场景下，自定义方案比通用框架更合适。