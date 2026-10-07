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

### 1.5 投递续期（heartbeat）：处理时间不可控时自己续命

消费者拿到一条消息后，如果在 ACK 超时到点前还没处理完，可以针对**这一条消息的本次投递**申请续期：

- `POST /api/subscriptions/{clientId}/heartbeat`，请求体 `{"messageId":"...","extendMs":5000}`。
  `extendMs` 省略或 <=0 时按 `broker.ack-timeout-ms` 续一个完整窗口；单次续期上限为
  `broker.heartbeat-max-extension-ms`（默认 60s），超过明确报 `400 INVALID_HEARTBEAT`，
  防止一条消息被无限挂住。响应返回新的绝对截止时刻 `deadlineAt`（epoch 毫秒）。
- 续期**只延长计时**：不把消息当已确认、不移除在途、不回退任何消费进度。
- 续期成功后在新的截止时刻前，超时扫描**不会**把这条消息误判为超时重投；不需要再拉取也能续。
- 续期只作用于"本次投递"：若续期后仍超时且未确认，按原规则重投（`deliveryAttempt` 照常累计，
  到 `broker.max-deliveries` 上限照常进死信）；重投后的新投递窗口重新计算，之前的续期不带过去。
- 非法续期全部明确报错（绝不当成成功）：

| 情况 | 结果 |
|---|---|
| 续期当前在途且已投递的消息 | `200`，返回新截止时刻 |
| 续期已确认的消息 | `400 ALREADY_ACKED` |
| 续期不存在的 messageId | `400 MESSAGE_NOT_FOUND` |
| 续期从未投递给本订阅端的消息 | `400 MESSAGE_NOT_FOUND`（说明主题不匹配/订阅更晚） |
| 续期在途但还没被 poll 取走的消息 | `400 NOT_YET_DELIVERED`（不存在需要续期的投递） |
| 续期已进死信的消息 | `400 DEAD_LETTERED`（并指引去 redrive） |
| 续期时长超过上限 | `400 INVALID_HEARTBEAT` |

### 1.6 死信再投递（redrive）：把死信捞回来重新消费

死信不只是能看，还能捞回来重新投给原订阅端：

- `POST /api/dead-letters/redrive`，请求体 `{"clientId":"...","messageIds":["id1","id2",...]}`。
- **整批原子**：任一条不合法（不存在 / 已确认 / 不在死信中）则整批拒绝，不产生部分生效。
- **顺序稳定**：批内按全局序号（即原发布顺序）重新进入在途窗口；入参乱序不影响结果。
  再投递的消息落回窗口中自己原来的序号位置，不插队、不打乱该订阅端在途消息的顺序。
- **重复触发安全**：同一批再次 redrive 时，消息已不在死信中，明确报 `400 NOT_DEAD_LETTERED`，
  不会成倍重复；`GET /api/dead-letters` 返回的每条记录带 `redriven`/`redrivenAt`，
  能直接看出哪些已经捞回过。
- **容量确定**：在途窗口放不下整批时明确报 `400 REDRIVE_CAPACITY_EXCEEDED`（不截断、不悄悄丢消息）；
  单批条数超过 `broker.max-redrive-batch`（默认 100）报 `400 REDRIVE_BATCH_TOO_LARGE`，分批即可。
- 再投递的消息按**全新一次投递**计时（`deliveryAttempt=1`），走正常的 poll/ACK/超时重投流程，
  确认后彻底完成；`deliveredSeq` 游标只增不减，订阅位置不回退。
- 对已经确认过的消息再投递报 `400 ALREADY_ACKED`，对不存在的消息报 `400 MESSAGE_NOT_FOUND`。

### 1.7 与原有投递/重投/积压规则的兼容范围

- 续期与再投递都复用同一把进程内串行锁，与发布、拉取、ACK、超时扫描天然并发安全；
  某个订阅端的续期/再投递只操作自己的在途集合，不会拖死其他订阅端。
- 续期不改变任何既有规则：超时窗口的起算方式、重投次数上限、死信条件、积压策略全部照旧，
  它只是把"本次投递"的截止时刻往后推。
- 再投递回来的消息占用 `broker.max-pending-per-subscription` 窗口容量，容量不够时明确拒绝，
  不会触发溢出策略去顶掉别人的在途消息；进入窗口后与正常消息走完全相同的
  poll/ACK/超时重投/死信路径。
- 死信台账只增不改：再投递不会删除台账记录，只把 `redriven` 标记置真，审计轨迹完整。

### 1.8 积压策略（行为确定、可解释）

单订阅端在途（未完成）消息数达到 `broker.max-pending-per-subscription` 后，新匹配消息由
`broker.overflow-strategy` 决定确定性行为，且被移出投递路径的消息**一律进入死信台账**：

- `dead-letter`（默认）：保留当前在途窗口内的消息不动，**拒绝新消息**并把新消息写入死信
  （原因 `PENDING_OVERFLOW:dead-letter(...)`）。优先保证窗口内消息最终完成。
  发布接口对这类情况返回 `status=OVERFLOW_DEADLETTER` 并在 `note` 中说明是哪些订阅端、上限是多少。
- `drop-oldest`：把该订阅端**最旧的在途消息**移入死信，腾位置给新消息
  （原因 `PENDING_OVERFLOW:drop-oldest(attempts=n)`）。优先追上最新流量，代价是牺牲最旧消息。

> 注意：溢出是"每个订阅端独立"发生的。同一条消息对快客户端正常投递，对已经积压的慢客户端才可能进死信。

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
| POST | `/api/subscriptions/{clientId}/heartbeat` | 投递续期 `{"messageId","extendMs"?}` → `{messageId,clientId,extendMs,deadlineAt}` |
| GET | `/api/dead-letters?clientId=` | 查询死信（不带 clientId 返回全部），每条含 `redriven`/`redrivenAt` |
| POST | `/api/dead-letters/redrive` | 死信再投递 `{"clientId","messageIds":[...]}` → `{clientId,count,messageIds,seqs}` |

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
| `broker.heartbeat-max-extension-ms` | 60000 | 单次投递续期的最大时长 |
| `broker.max-redrive-batch` | 100 | 单次死信再投递的最大批量 |

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
# poll 拿到 messageId 后、ACK 超时到点前续期 30s（处理还没完）
curl -s -X POST localhost:8080/api/subscriptions/device-1/heartbeat \
  -H 'Content-Type: application/json' \
  -d '{"messageId":"<messageId>","extendMs":30000}'
# 响应含新的 deadlineAt；此前调度器不会把这条消息超时重投
# 对同一条重复调用可继续续；ACK 之后再续期会得到 400 ALREADY_ACKED
```

### 观察死信再投递

```bash
# 先用小超时制造一条死信（见上一节），然后把它捞回来：
curl -s -X POST localhost:8080/api/dead-letters/redrive \
  -H 'Content-Type: application/json' \
  -d '{"clientId":"device-1","messageIds":["<死信的messageId>"]}'
# -> {"count":1,...}；再次 poll 能重新收到（deliveryAttempt=1），ACK 后彻底完成
# 再查 /api/dead-letters?clientId=device-1：该条 redriven=true
# 重复执行同一 redrive -> 400 NOT_DEAD_LETTERED，不会重复投递
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

控制台日志以 `场景一`～`场景七` 开头，直接标明覆盖的业务场景与每个用例的动作/结果：

- 场景一：主题过滤、非法订阅拒绝、未知订阅端报错
- 场景二：断线重连续传、进度不回退、同主题 8 线程并发发布顺序稳定
- 场景三：至少一次、超时重投、超上限进死信、重复/伪造/越权 ACK 全部明确报错
- 场景四：dead-letter 与 drop-oldest 两种积压策略、慢订阅端不影响快订阅端
- 场景五：投递续期——续期后不误判超时、续期过期仍按原规则重投/进死信、续期≠确认、
  五类非法续期明确报错、8 线程并发续期+扫描+发布互不干扰
- 场景六：死信再投递——捞回后正常收到并 ACK、重复再投拒绝且不成倍、批量保序、
  落回原序号位置不插队、整批原子拒绝、容量上限明确拒绝、4 线程并发再投同一批仅一次成功
- 场景七：完整 HTTP 端到端（真实调度器重投 → 死信）、续期与再投递 REST 全流程、
  冲突 409、畸形 JSON 400
