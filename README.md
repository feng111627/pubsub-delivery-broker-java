# pubsub-delivery-broker-java

一个本地可运行的发布订阅消息中间件（Spring Boot，内存存储），重点实现扎实的投递语义：
至少一次投递、确认/重投/死信、断线续传、订阅端隔离与稳定顺序。

## 快速开始

```bash
mvn spring-boot:run        # 默认端口 8080
mvn test                   # 运行全部单测，日志会打印每个业务场景及结果
```

## 投递语义

- **至少一次**：消息只有被订阅端确认（ack）后才算完成；未确认的投递不会丢。
- **确认（ack）**：`poll` 拉到的每条投递有 `deliveryId` 和确认截止时间，需在
  `broker.ack-timeout` 内确认。对**不存在的投递**确认返回 `DELIVERY_NOT_FOUND`(404)，
  对**已确认过的投递**重复确认返回 `ALREADY_ACKED`(409)，不会当成成功。
- **超时重投**：超时未确认的投递由后台调度（`broker.redelivery-scan-interval`）退回队列重投，
  `attempt` 计数递增。
- **死信**：投递次数达到 `broker.max-delivery-attempts` 仍未确认，消息进入死信存储，
  可通过 `GET /broker/dead-letters` 查询（含原因、尝试次数），不会凭空消失。
- **断线续传**：订阅以名字标识。同名同过滤器重新订阅即视为重连：断线期间未确认的投递
  立即退回队列重新可拉取，消费进度（每主题已确认的最大序号）保留，且**只进不退**——
  迟到的旧确认不会把进度拉回去。同名但过滤器不同的订阅请求会被拒绝并说明原因。
- **订阅端隔离**：每个订阅端有独立的待投递/在途队列和锁，慢订阅端只会积压自己，
  不影响其他订阅端的投递。
- **积压策略**：单订阅端积压（待投递+在途）达到 `broker.max-backlog` 时，按
  `broker.backlog-strategy` 处理：
  - `DEAD_LETTER`（默认）：溢出的消息落死信（原因 `BACKLOG_OVERFLOW`），可查，不阻塞发布；
  - `REJECT`：发布请求被明确拒绝，返回 `BACKLOG_FULL`(429)。
- **顺序**：同一主题的每条消息在条纹锁内分配单调递增序号并按序入队，
  高并发发布下订阅端按序号顺序收到，序号无重无漏。

## 主题匹配规则

- 主题以 `/` 分段，如 `orders/eu/paid`；段不能为空，主题不能为空。
- 订阅过滤器支持通配符：
  - `+`：恰好匹配一段（`orders/+/paid` 匹配 `orders/eu/paid`，不匹配 `orders/eu/west/paid`）；
  - `#`：匹配剩余任意多段（含零段），只能出现在最后一段（`orders/#` 匹配 `orders` 及 `orders/eu/paid`）。
- 通配符必须独占一段（`pa+`、`or#ders` 非法）；发布主题不允许包含通配符。
- 非法订阅请求返回 `INVALID_TOPIC`(400)，响应体里说明具体问题。

## 配置项（application.properties，`broker.*`）

| 配置 | 默认 | 说明 |
|---|---|---|
| `broker.ack-timeout` | 5s | 确认超时，超时未 ack 的投递会被重投 |
| `broker.max-delivery-attempts` | 5 | 最大投递次数（含首次），超过落死信 |
| `broker.max-backlog` | 1000 | 单订阅端最大积压（待投递+在途） |
| `broker.backlog-strategy` | DEAD_LETTER | 积压策略：`DEAD_LETTER` 或 `REJECT` |
| `broker.redelivery-scan-interval` | 0.5s | 超时重投扫描间隔 |

## REST API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/broker/publish` | 发布 `{topic, payload}`，返回 `{messageId, topic, seq}` |
| POST | `/broker/subscribe` | 订阅/重连 `{name, filter}` |
| POST | `/broker/subscriptions/{name}/poll?max=N` | 拉取最多 N 条投递 |
| POST | `/broker/subscriptions/{name}/ack` | 确认 `{deliveryId}` |
| GET | `/broker/dead-letters?subscription=X` | 查询死信（可按订阅端过滤） |

错误响应统一为 `{"error": "<错误码>", "message": "<具体原因>"}`。

## 本地验证

```bash
# 1. 订阅（支持通配符）
curl -X POST localhost:8080/broker/subscribe -H 'Content-Type: application/json' \
  -d '{"name":"sub-1","filter":"orders/+"}'

# 2. 发布
curl -X POST localhost:8080/broker/publish -H 'Content-Type: application/json' \
  -d '{"topic":"orders/created","payload":"hello"}'

# 3. 拉取（拿到 deliveryId）
curl -X POST 'localhost:8080/broker/subscriptions/sub-1/poll?max=10'

# 4. 确认（把 <deliveryId> 换成上一步的值）
curl -X POST localhost:8080/broker/subscriptions/sub-1/ack -H 'Content-Type: application/json' \
  -d '{"deliveryId":"<deliveryId>"}'

# 5. 验证重投：拉取后不确认，等 ack-timeout 后再次 poll 会看到同一条消息（attempt 递增）
# 6. 验证死信：持续不确认，超过 max-delivery-attempts 后：
curl 'localhost:8080/broker/dead-letters?subscription=sub-1'

# 7. 验证断线续传：不确认直接重新执行第 1 步（同名同过滤器），再 poll 即可继续收
```

## 测试

`mvn test` 覆盖：主题匹配与非法拒绝、主流程发布/确认、重复确认与不存在确认的报错、
超时重投、死信、断线续传与进度不回退、慢订阅端隔离、积压两种策略、高并发下同主题顺序稳定。
测试日志中每个场景以 `[场景] <描述> => 通过/失败` 打印。
