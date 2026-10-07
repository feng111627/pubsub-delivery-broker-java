# Pub/Sub Delivery Broker（本地发布订阅消息中间件）

一个可在本地直接跑起来的轻量 Pub/Sub Broker（Spring Boot，内存存储，无外部依赖）。
重点不是功能多，而是把**投递语义**做扎实：断线不丢、不乱序、进度不回退，异常情况都有确定且可解释的行为。

---

## 1. 投递语义

### 1.1 至少一次（at-least-once）+ 显式 ACK

- 消息只有在订阅端显式 ACK 后才算完成；未 ACK 的消息一直留在该订阅端的在途集合里。
- 投递后超过 `broker.ack-timeout-ms` 仍未 ACK，调度器自动重投：
  - 返回的 `messageId` 不变，`deliveryAttempt` 递增（1 为首次，>1 表示重投），`redelivery=true`。
  - 消费端必须按 `messageId` 幂等处理（允许重复投递，但同一条不会出现两个不同 ID）。
- 投递次数达到 `broker.max-deliveries` 仍未 ACK：**不再无限重投，转入死信台账**（消息不消失），
  通过 `GET /api/dead-letters` 可按订阅端查询，原因记录为 `MAX_DELIVERIES_EXCEEDED(attempts=n, timeoutMs=...)`。
- 超时计时从消息**真正被 poll 取走**开始；订阅端一直在线轮询时会持续刷新计时窗口，
  尚未被任何 poll 取走的消息不参与超时（不会出现"还没收到就被重投"）。

### 1.2 断线重连：从上次位置继续，进度不回退

- 订阅身份由 `clientId` 标识。断线后用**同一个 clientId + 同一个 filter** 重新订阅，即恢复原订阅状态。
- 每条消息在每个订阅端都有 Broker 全局单调序号 `seq`：
  - 已投递游标 `deliveredSeq` 只增不减；
  - 未 ACK 的消息超时后以**相同 seq、相同 messageId**重投，消费端看到的顺序与进度都不会倒退。
- 新订阅从"订阅那一刻"的最新位置开始，不收历史消息。
- 同一 `clientId` 用不同 filter 重新订阅会被拒绝（`409 SUBSCRIPTION_CONFLICT`），
  防止误连导致语义错乱；重连请复用原 filter。

### 1.3 ACK 的严格校验（绝不把错误当成功）

| 情况 | 结果 |
|---|---|
| ACK 当前在途消息 | `204`，消息完成，后续不再重投 |
| 重复 ACK 已确认消息 | `400 ALREADY_ACKED`（错误信息含 clientId/messageId/seq） |
| ACK 不存在的 messageId | `400 MESSAGE_NOT_FOUND` |
| ACK 从未投递给本订阅端的消息（主题不匹配/订阅更晚） | `400 MESSAGE_NOT_FOUND` |
| ACK 已进死信的消息 | `400 DEAD_LETTERED` |
| 未知 clientId 调用 poll/ack | `400 MESSAGE_NOT_FOUND` |

### 1.4 订阅端隔离与顺序保证

- 每个订阅端有独立的在途集合；慢客户端只在自己的集合上积压/触发策略，**不会阻塞或拖死其他订阅端**。
- 发布在 Broker 内全局串行化：同一主题即使高并发发布，每个订阅端收到的消息也严格按
  全局序号（等同主题内发布顺序）升序，无重复、无跳号（未触发溢出策略时）。
- 轮询返回顺序：待重投消息优先，其次首次投递；两类内部均按 `seq` 升序。

### 1.5 积压策略（行为确定、可解释）

单订阅端在途（未完成）消息数达到 `broker.max-pending-per-subscription` 后，新匹配消息由
`broker.overflow-strategy` 决定确定性行为，且被移出投递路径的消息**一律进入死信台账**：

- `dead-letter`（默认）：保留当前在途窗口内的消息不动，**拒绝新消息**并把新消息写入死信
  （原因 `PENDING_OVERFLOW:dead-letter(...)`）。优先保证窗口内消息最终完成。
  发布接口对这类情况返回 `status=OVERFLOW_DEADLETTER` 并在 `note` 中说明是哪些订阅端、上限是多少。
- `drop-oldest`：把该订阅端**最旧的在途消息**移入死信，腾位置给新消息
  （原因 `PENDING_OVERFLOW:drop-oldest(attempts=n)`）。优先追上最新流量，代价是牺牲最旧消息。

> 注意：溢出是"每个订阅端独立"发生的。同一条消息对快客户端正常投递，对已经积压的慢客户端才可能进死信。

### 1.6 投递续期（消费者自己续命）

消费者拿到一条消息后，如果在本次确认超时到点之前还没处理完，可以单独申请把这次投递的
超时往后延（`POST /api/subscriptions/{clientId}/extend`，`extendMs` 为空时按
`broker.ack-timeout-ms` 延长）。**不需要再拉取也能续期**。

- 续期成功后，在新的到点之前该消息**不会被超时扫描误判重投**；续期窗口到期后仍未 ACK 的，
  按原规则重投，`deliveryAttempt` 照常累计，到 `broker.max-deliveries` 上限照常进死信。
- 续期**不等于确认**：不消耗/不改变投递次数，消息仍在在途集合里，消费进度不回退；
  处理完仍需正常 ACK。
- 续期只延不缩：同一投递窗口内多次续期，到点单调后移，不会被更短的续期提前。
- 续期只作用于**本次投递窗口**：一旦发生重投（新一轮计时），之前的续期自动失效。
- 单次续期长度必须为正且不超过 `broker.max-extend-ms`，否则 `400 INVALID_EXTENSION`。
- 明确报错的情形（绝不当成成功）：

| 情况 | 结果 |
|---|---|
| 续期当前在途消息 | `200`，返回新的超时到点 `extendedUntil` |
| 续期已确认消息 | `400 ALREADY_ACKED` |
| 续期已进死信的消息 | `400 DEAD_LETTERED`（提示改走死信再投递） |
| 续期从未投递给本订阅端的消息 | `400 MESSAGE_NOT_FOUND` |
| 续期不存在的 messageId / 未知 clientId | `400 MESSAGE_NOT_FOUND` |
| `extendMs <= 0` 或超过 `broker.max-extend-ms` | `400 INVALID_EXTENSION` |

### 1.7 死信再投递（把死信捞回来）

死信不只是能看：`POST /api/dead-letters/redeliver`（`{"clientId","messageIds":[...]}`）
把死信台账中**还没真正被消费掉**的消息重新投给对应订阅端，消费端按正常流程
（poll → 处理 → ACK）再收到并确认。

- **顺序稳定**：一批消息无论请求里什么顺序，实际按全局序号（即发布先后）升序重新入队；
  投出时走既有"重投优先、序号升序"规则，不会在订阅端已经在走的顺序里插队。
- **幂等**：同一批重复触发（包括并发重复触发），已捞回的消息报告
  `ALREADY_REDELIVERED`，不重复入队、不成倍重复；台账里 `redeliveredAt>0` 表示已捞过，
  避免反复捞导致越捞越多。
- **新一轮投递周期**：捞回的消息 `messageId`/`seq` 不变，`deliveryAttempt` 从 1 重新计，
  重新享有完整的 `broker.max-deliveries` 次投递机会；消费进度游标只增不减。
- **整批原子**：批里任何一条校验失败（不存在/已确认/不在死信中），整批拒绝，
  任何消息都不会被捞回。
- **容量边界（确定且可解释）**：捞回后该订阅端在途总数若会超过
  `broker.max-pending-per-subscription`，整批拒绝并返回 `429 PENDING_FULL`
  （错误信息含当前在途数、待捞回数与上限），消息留在死信台账里**不会丢**；
  先 ACK 腾出容量或缩小批量后重试即可。
- 明确拒绝的情形：

| 情况 | 结果 |
|---|---|
| 死信台账中未捞回的消息 | `200`，`status=REDELIVERED` |
| 台账中已捞回过的消息（重复触发） | `200`，`status=ALREADY_REDELIVERED`（幂等，不重复投） |
| 消息已被该订阅端确认 | `400 ALREADY_ACKED` |
| 消息不存在 / 从未投递给该订阅端 | `400 MESSAGE_NOT_FOUND` |
| 消息正在投递途中（不在死信里） | `400 NOT_DEAD_LETTERED` |
| 空批次 / 空 messageId | `400 INVALID_REQUEST` |
| 捞回后超出在途上限 | `429 PENDING_FULL`（整批拒绝，不丢消息） |

### 1.8 与原有投递/重投/积压规则的兼容范围

- 续期、再投递与正常发布/拉取/确认**共享同一把 Broker 全局锁**，全部状态变更串行化：
  并发进行时不产生中间态，慢订阅端的续期/捞回只影响它自己的在途集合，不拖死其他订阅端。
- 续期只推迟超时到点，不改变积压计数：在途上限、溢出策略（`dead-letter`/`drop-oldest`）
  的语义完全不变。
- 死信再投递受在途上限约束（见 1.7 的 `PENDING_FULL`），这是对原有积压规则的唯一新增交互：
  宁可明确拒绝，也不悄悄丢消息。
- 捞回后再次超时仍按 `MAX_DELIVERIES_EXCEEDED` 进死信（台账记录刷新为最新原因与时间），
  可以再次捞回——死信台账按 (clientId, messageId) 去重，记录数不膨胀。

---

## 2. 主题匹配规则

层级式主题，用 `/` 分层（如 `orders/new/york/2026`）。

- 每层非空；不允许前导 `/`、结尾 `/`、连续 `//`；层内不允许空白字符；长度上限 256。
- 订阅 filter 支持 MQTT 风格通配符：
  - `+`：**独占一层**，恰好匹配任意一层。`orders/+/shipped` 匹配 `orders/new/shipped`，
    不匹配 `orders/new/york/shipped`。
  - `#`：**只能独占最后一层**，匹配零层或任意多层后缀。
    `orders/#` 匹配 `orders`、`orders/x`、`orders/x/y/z`；`#` 匹配一切。
  - `a+`、`a#`、`x/#/y` 这类写法明确拒绝，错误信息指出层级位置与原因（错误码 `INVALID_SUBSCRIPTION`）。
- 发布主题是具体主题，**不允许出现 `+`/`#`**。

---

## 3. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/publish` | 发布消息 `{"topic","payload"}` → `{messageId,globalSeq,topicSeq,status,note}` |
| POST | `/api/subscribe` | 订阅/重连 `{"clientId","filter"}` → `{clientId,filter,position}` |
| POST | `/api/subscriptions/{clientId}/poll?max=32` | 拉取消息（短轮询，建议客户端循环调用） |
| POST | `/api/subscriptions/{clientId}/ack` | 确认 `{"messageId"}` → `204` |
| POST | `/api/subscriptions/{clientId}/extend` | 投递续期 `{"messageId","extendMs"}` → `{messageId,seq,extendedUntil}` |
| GET | `/api/dead-letters?clientId=` | 查询死信（不带 clientId 返回全部；`redeliveredAt>0` 表示已捞回） |
| POST | `/api/dead-letters/redeliver` | 死信再投递 `{"clientId","messageIds":[...]}` → `{redelivered,alreadyRedelivered,items}` |

投递对象字段：`messageId, topic, payload, seq, deliveryAttempt, redelivery`。

错误响应统一为：`{"errorCode": "...", "message": "..."}`。

---

## 4. 配置项（`src/main/resources/application.properties`）

| 配置项 | 默认值 | 含义 |
|---|---|---|
| `broker.ack-timeout-ms` | 10000 | ACK 超时，超时触发重投 |
| `broker.max-deliveries` | 3 | 最大投递次数（含首次），达到后进死信 |
| `broker.max-pending-per-subscription` | 100 | 单订阅端在途消息上限 |
| `broker.overflow-strategy` | dead-letter | 积压策略：`dead-letter` / `drop-oldest` |
| `broker.redelivery-scan-ms` | 200 | 重投扫描间隔 |
| `broker.max-extend-ms` | 60000 | 单次投递续期允许的最大延长毫秒数 |

存储为进程内内存结构：重启 Broker 会清空状态（`clientId` 的续传语义针对的是**断线重连**，不是 Broker 重启）。

---

## 5. 本地运行与验证

### 启动

```bash
mvn spring-boot:run
# 服务监听 http://localhost:8080
```

### 端到端验证（curl）

```bash
# 1) 订阅（clientId=device-1，filter 用通配符）
curl -s -X POST localhost:8080/api/subscribe \
  -H 'Content-Type: application/json' \
  -d '{"clientId":"device-1","filter":"orders/#"}'

# 2) 发布两条消息
curl -s -X POST localhost:8080/api/publish \
  -H 'Content-Type: application/json' -d '{"topic":"orders/new","payload":"hello"}'
curl -s -X POST localhost:8080/api/publish \
  -H 'Content-Type: application/json' -d '{"topic":"payment/pay","payload":"ignored"}'

# 3) 拉取：只能收到匹配 orders/# 的一条；返回里含 messageId
curl -s -X POST 'localhost:8080/api/subscriptions/device-1/poll?max=10'

# 4) 用返回的 messageId 确认（成功返回 204）
curl -i -X POST localhost:8080/api/subscriptions/device-1/ack \
  -H 'Content-Type: application/json' -d '{"messageId":"<上一步的messageId>"}'

# 5) 重复确认 -> 400 ALREADY_ACKED；伪造 ID -> 400 MESSAGE_NOT_FOUND
curl -s -X POST localhost:8080/api/subscriptions/device-1/ack \
  -H 'Content-Type: application/json' -d '{"messageId":"<同一个messageId>"}'

# 6) 非法订阅 -> 400，message 明确指出问题（这里是空层级）
curl -s -X POST localhost:8080/api/subscribe \
  -H 'Content-Type: application/json' -d '{"clientId":"bad","filter":"orders//x"}'

# 7) 死信查询
curl -s 'localhost:8080/api/dead-letters?clientId=device-1'
```

### 观察投递续期

```bash
# 订阅 -> 发布 -> poll 拿到 messageId 后，处理不完就续期 30s（无需再拉取）
curl -s -X POST localhost:8080/api/subscriptions/device-1/extend \
  -H 'Content-Type: application/json' \
  -d '{"messageId":"<messageId>","extendMs":30000}'
# 返回 {"messageId":...,"seq":...,"extendedUntil":<新超时到点>}
# 在 extendedUntil 之前不会发生超时重投；到期仍未 ACK 则照常重投/进死信。
# 对已确认或不存在的 messageId 续期会返回 400 并说明原因。
```

### 观察死信再投递

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--broker.ack-timeout-ms=1000 --broker.max-deliveries=2"
# 订阅 -> 发布 -> poll 一次后不 ACK，等消息进死信（/api/dead-letters 可查）

# 把死信捞回来（可一次一批，顺序按发布先后）
curl -s -X POST localhost:8080/api/dead-letters/redeliver \
  -H 'Content-Type: application/json' \
  -d '{"clientId":"device-1","messageIds":["<messageId>"]}'
# 返回 {"redelivered":1,"alreadyRedelivered":0,"items":[{"messageId":...,"status":"REDELIVERED"}]}

# 再次 poll 即可按正常流程收到并 ACK；死信台账里该记录 redeliveredAt>0 表示已捞过。
# 同一批重复执行上面的 redeliver：redelivered=0、alreadyRedelivered=1，不会重复投递。
```

### 观察超时重投与死信

用小超时启动，然后"poll 一次拿到消息但不 ACK、断开不再 poll"：

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--broker.ack-timeout-ms=1000 --broker.max-deliveries=2"
# 订阅 -> 发布 -> poll 一次（拿到消息）-> 等待 >1s 后再 poll：可见 deliveryAttempt=2, redelivery=true
# 再等一个周期不 ACK：消息从 poll 中消失，出现在 /api/dead-letters，reason=MAX_DELIVERIES_EXCEEDED
```

### 观察慢订阅端积压

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--broker.max-pending-per-subscription=2"
# 订阅 clientId=slow 后连续发布 3 条匹配消息且不 ACK：
# 第 3 条发布响应 status=OVERFLOW_DEADLETTER；/api/dead-letters?clientId=slow 可查到该消息；
# poll 仍稳定返回窗口内最旧两条。另开一个 clientId=fast 边发边 ACK，不受 slow 影响。
```

### 运行测试

```bash
mvn test
```

控制台日志以 `场景一`～`场景五` 开头，直接标明覆盖的业务场景与每个用例的动作/结果：

- 场景一：主题过滤、非法订阅拒绝、未知订阅端报错
- 场景二：断线重连续传、进度不回退、同主题 8 线程并发发布顺序稳定
- 场景三：至少一次、超时重投、超上限进死信、重复/伪造/越权 ACK 全部明确报错
- 场景四：dead-letter 与 drop-oldest 两种积压策略、慢订阅端不影响快订阅端
- 场景五：完整 HTTP 端到端（真实调度器重投 → 死信）、冲突 409、畸形 JSON 400
- 场景六：投递续期——续期窗口内不误判超时、续期只延不缩、续期到期后照常重投并累计进死信、
  已确认/死信/从未投递/不存在消息续期与非法时长全部明确报错
- 场景七：死信再投递——捞回后按发布先后投出并可正常 ACK、重复触发幂等不翻倍、
  已确认/不存在/非死信消息明确拒绝、整批原子、满员 PENDING_FULL 不丢消息、
  并发重复触发+并发发布下恰好捞回一次且整体 seq 升序、续期/再投与发布拉取并发互不拖死
- 场景八（HTTP）：续期与死信再投递的 REST 端到端，含 400/429 错误码与台账 redeliveredAt 标记
