# Akzio v2 Rust 学习指南

> 本文是给第一次阅读本项目的 Rust 初级开发者的导航。它解释代码中实际出现的语法和机制；示例分为“项目真实实现”和“独立教学示例”，不能把教学示例当成项目行为。业务事实以当前源码、运行时契约和 Store/CAS 记录为准。

## 0. 先建立阅读地图

Akzio 是一个 Rust workspace，而不是一个单独的二进制文件。根 `Cargo.toml` 把领域模型、Store、Context、Ingest、Runtime、Execution、Learning、Model、Research、Daemon 和 CLI 组合在一起；`rust-toolchain.toml` 固定 Rust 1.96.0。建议按下面的真实数据流阅读：

```text
CLI / Observatory
    -> loopback HTTP 与认证
    -> Daemon / Scheduler / WorkflowRuntime
    -> Evidence 与 ContextBroker
    -> Research Agent 的受控提交
    -> Domain 校验与 Store/CAS 持久化
    -> DecisionGate
    -> ExecutionGate / Paper Commitment / Reconcile
    -> Outcome 与 Learning
```

对应的主要入口和边界如下：

| 层 | 主要路径 | 阅读重点 |
| --- | --- | --- |
| 领域与协议 | `crates/akzio-domain/src/` | 纯数据结构、哈希、校验、状态和版本身份；这里不应有 I/O。 |
| 唯一持久化层 | `crates/akzio-store/src/` | SQLite 连接、CAS BLOB、Artifact 引用、事务边界、lease 和恢复。 |
| Context 沙箱 | `crates/akzio-context/src/` | Manifest、ReadGrant、投影和授权边界；Agent 不直接读文件或 SQL。 |
| 研究与运行时 | `crates/akzio-research/src/`、`crates/akzio-runtime/src/` | Contract、Prompt、预算、Future、节点生命周期和恢复。 |
| 决策与 Paper | `crates/akzio-execution/src/` | Decision/Execution 两个 Gate、Commitment、Paper endpoint 和对账。 |
| 进程与入口 | `crates/akzio-daemon/src/`、`crates/akzio-cli/src/` | 调度、HTTP、认证、控制请求以及 CLI 到 Core 的链路。 |
| 证据、模型、学习 | `crates/akzio-ingest/src/`、`crates/akzio-model/src/`、`crates/akzio-learning/src/` | 外部读取、响应解析、Outcome 评估和学习资格。 |

业务上必须保持几个区分：研究提案不是 Decision；Decision 的目标也不是订单；ExecutionVerdict 不是 Paper 提交；订单 accepted 不是成交；成交不是 Outcome；Sealed Outcome 也不自动意味着 Policy 已激活。代码旁的注释应该解释这些边界，而不是把相邻阶段合并成“完成”。

## 1. 绑定、类型、模块和模式匹配

### 1.1 变量、可变性、结构体和枚举

Rust 变量默认不可变。`let x = ...` 允许读取但不允许重新赋值；`let mut x = ...` 允许在同一个所有者仍持有值时修改它。`struct` 把有名字的字段组合成一个值，`enum` 把互斥的几种状态组合成一个类型。

项目真实实现：`akzio-domain` 中的 `RunPurpose`、`WorkflowStatus`、`ExecutionVerdict` 等枚举把合法状态显式化；`akzio-context/src/broker.rs` 中的 `ContextManifest`、`ContextDocumentMetadata` 等结构体承载授权投影；`akzio-model/src/lib.rs` 中的 `ModelRequest` 和 `ModelResponse` 表示模型协议边界。读取这些类型时，先看字段，再看 `validate` 或构造函数，最后看调用方如何匹配它们。

```rust
// 独立教学示例：枚举的每个变体携带不同信息。
enum ResultState {
    Accepted { id: String },
    Rejected { reason: String },
}

fn describe(state: ResultState) -> String {
    match state {
        ResultState::Accepted { id } => format!("已受理: {id}"),
        ResultState::Rejected { reason } => format!("已拒绝: {reason}"),
    }
}
```

`match` 必须穷尽所有变体；这会把新增状态变成编译期提醒。`if let` 只处理一个感兴趣的变体，其他变体被忽略。解构时绑定默认取得值本身，可能发生 Move；对引用匹配则得到借用，是否移动要看匹配对象的类型和模式写法。

### 1.2 `Option<T>`、`Result<T, E>` 和 `?`

`Option<T>` 表示“可能有一个 `T`，也可能没有”：`Some(value)` 与 `None` 的含义由当前业务定义。`Result<T, E>` 表示成功值或错误值：`Ok(value)` 与 `Err(error)` 不等价于“业务全流程完成”。

项目真实实现：Store 的读取函数常返回 `StoreResult<Option<...>>`，外层 `Err` 表示数据库或一致性失败，`Ok(None)` 则表示查询成功但没有匹配记录。`akzio-execution` 的 Gate 返回 `Result` 时，`Ok(NoOrder)` 仍可能是一个明确的业务判定；`akzio-daemon` 的 HTTP handler 还要把这个判定转换成响应，而不是把 `Ok` 解释成 Paper 成交。

`?` 做两件事：成功时取出 `T` 继续；失败时立刻从当前函数返回 `Err`，必要时利用 `From` 把错误转换成当前函数的错误类型。它只传播当前调用层，不会自动重试、回滚已经完成的外部副作用，也不表示所有子任务都成功。

```rust
// 独立教学示例：两个失败来源都在当前函数边界汇合。
fn read_name(value: Option<&str>) -> Result<String, String> {
    let raw = value.ok_or_else(|| "缺少名称".to_owned())?;
    if raw.is_empty() {
        return Err("名称为空".to_owned());
    }
    Ok(raw.to_owned())
}
```

常见误区：用 `unwrap` 把外部输入或数据库缺失当成必然存在；用 `expect` 的文字掩盖未验证的前置条件；在批处理循环里用 `?`，却没有确认这会让整批提前失败还是只让当前单项失败。项目中的注释应指出错误传播层级、是否已持久化以及失败后留下的历史。

## 2. 所有权、Move、借用和资源释放

### 2.1 Ownership、Move、Copy、Clone

每个 Rust 值有一个所有者。把非 `Copy` 值赋给另一个变量、作为值参数传入或从函数返回，通常会 Move，原绑定不再可用；`Copy` 类型（如整数和某些小的标量）按位复制；`Clone` 是显式复制，成本和语义由实现决定，不能默认认为便宜。

项目真实实现：`Store`、运行时配置和领域 ID 经常被 `clone` 后交给异步任务或新的阶段；这表示实现需要另一个所有权句柄，并不意味着底层 SQLite、Artifact 或业务记录被复制。`DecisionGate::decide(&DecisionGateInput)` 通过共享借用读取输入，调用者仍保留输入；`&mut self` 方法则暂时独占借用接收者以修改其状态。

```rust
// 独立教学示例：String 被移动，&str 只借用。
let name = String::from("TQQQ");
let view: &str = &name;
println!("{view}");
// name 仍可用，因为 view 的借用已经结束或尚未超过这里。
```

### 2.2 `&T`、`&mut T`、NLL、reborrow 和 partial move

`&T` 是共享借用：同一时间可以有多个读取者；`&mut T` 是独占借用：借用期间不能再有其他会冲突的读写借用。借用检查器会根据实际最后一次使用进行 NLL（Non-Lexical Lifetimes，非词法生命周期）分析，所以借用有时会在代码块结束前就结束。

reborrow（再借用）是从已有的 `&mut T` 暂时生成更短的 `&mut` 或 `&T`，让嵌套调用使用一小段独占权限，随后原来的 `&mut` 可以继续使用。partial move（部分移动）发生在从非 `Copy` 结构体中移动一个字段后：没有被移动的字段仍可能可用，但整个结构体通常不能再整体使用。

项目真实实现：`akzio-context/src/context_broker/materialization.rs` 对 `serde_json::Value` 使用 `&mut Value` 修改投影；`akzio-store` 的连接辅助类型以借用的 `MutexGuard` 持有锁保护的连接，借用范围决定何时释放锁。注释应说明“这次借用保护哪个状态、在哪里结束”，而不是只写“这里借用了”。

### 2.3 Drop、RAII 和锁释放

Rust 用 `Drop` 和 RAII（Resource Acquisition Is Initialization）在值离开作用域时释放资源。`MutexGuard` 离开作用域会解锁；文件、网络响应和数据库事务的包装类型也会在 Drop 时执行相应清理，但“释放”不等于外部业务已经完成。

项目真实实现：`akzio-store/src/store/prelude.rs` 通过共享的 SQLite 连接和互斥保护访问；`akzio-runtime/src/runtime/store_executor.rs` 用 semaphore permit 和计数器约束 Store 操作；`akzio-daemon` 的 lease guard/任务结束路径会影响调度权。阅读这些代码时，检查 guard 是否跨 `await`、Drop 是否会写事件、取消是否留下 abandoned 或 retry 状态。

## 3. 生命周期与引用约束

生命周期（lifetime）描述引用至少要活多久。它不是“给对象加一个运行时计时器”，而是编译期对引用有效范围的约束。生命周期省略规则让常见函数可以省略 `'a`，但多个输入引用时返回引用的来源仍受规则约束。

项目真实实现：

- `akzio-context/src/broker.rs` 的 `ParentContextProof<'a>` 使用显式生命周期，表示证明结构借用外部上下文而不拥有它。
- `akzio-research/src/quality.rs` 的 `CountedModel<'a>` 和 `turn<'a>(&'a self, ...) -> BoxFuture<'a, ...>` 把 Future 的有效期绑定到模型对象的借用期。
- `akzio-cli/src/main.rs` 的 `FreezeRequest<'a>` 借用冻结请求中的文本或配置。
- `akzio-runtime/src/runtime/node.rs` 和 `akzio-daemon/src/scheduler.rs` 返回 `Pin<Box<dyn Future<...> + Send + 'a>>`，`'a` 约束 Future 不能持有超过输入借用期的引用。

`'static` 表示引用在整个程序期间有效，或者更常见地表示一个拥有的数据类型不借用短生命周期；`T: 'static` 不等于 `&'static T`。被 `tokio::spawn` 取得的任务通常必须满足足够长的生命周期，因为任务可能在当前栈帧返回后仍运行。

项目中未发现需要用 HRTB（Higher-Ranked Trait Bound，`for<'a>`）来表达通用借用回调的核心生产接口；如果以后出现 `for<'a>`，它表示“对任意生命周期 `'a` 都成立”，不是把一个固定生命周期命名为 `'a`。项目中也未发现显式 variance（生命周期方差）声明；方差是编译器判断生命周期替换安全性的类型系统规则，不应凭直觉改写生命周期。

## 4. Trait、泛型、约束与分发

Trait 是一组行为约束。泛型参数让一个函数或类型适用于多种具体类型；`T: Trait`、`where T: Trait` 是 trait bound（Trait 约束），要求调用者提供满足行为的类型。`impl Trait` 既可以表示“返回某个由编译器确定的具体实现”，也可以用于参数位置表达约束；`dyn Trait` 是运行时动态分发的 trait object。

项目真实实现：`akzio-ingest/src/adapters.rs` 的 `SourceDocumentFetcher`、`akzio-ingest/src/runtime.rs` 的 `AsyncEvidenceAdapter`、`akzio-runtime/src/runtime/node.rs` 的节点执行 trait，以及 `akzio-daemon/src/scheduler.rs` 的 scheduler 接口，把真实网络、fixture 和调度实现隔离开。调用方持有 `Arc<dyn AsyncEvidenceAdapter>` 或 `Pin<Box<dyn Future + Send + 'a>>` 时，只依赖 trait 公开的行为，而不依赖具体实现；代价是动态分发和对象安全约束。

项目中常见的泛型形式包括 `impl Into<String>`：调用者可以传入多种可转换为 `String` 的类型，函数内部取得转换后的所有权；`Vec<T>`、`Result<T, E>` 和 `Option<T>` 则把数据类型作为参数传入容器。`where` 适合把复杂约束放到签名下方，提高阅读性。

Associated Type（关联类型）在本项目的关键公开路径中不是主要建模方式；GAT（Generic Associated Type）未发现实际使用，下面是独立示例：

```rust
// 独立教学示例：GAT 允许关联类型自身带生命周期参数。
trait Windowed {
    type Window<'a> where Self: 'a;
    fn window(&self) -> Self::Window<'_>;
}
```

Object Safety（对象安全，较新的术语也称 dyn 兼容性）要求 trait 的方法能够在不知道具体 `Self` 大小和类型时调用。返回 `Self`、带有不受约束的泛型方法等设计可能使 trait 不能作为 `dyn Trait`。项目中的 adapter trait 因而把异步结果装箱为 Future；不能只把普通泛型函数机械改成 `dyn`。

静态分发（泛型/`impl Trait`）通常在编译期单态化（monomorphization），动态分发（`dyn Trait`）在运行时通过 vtable 调用。两者哪一个更快不能只由语法断言，必须结合编译产物和测量。

## 5. 智能指针、内部可变性和线程约束

### 5.1 `Box`、`Arc`、`Mutex`、`RwLock`、`Cell` 和 `RefCell`

`Box<T>` 把值放到堆上并提供唯一所有权；`Arc<T>`（Atomically Reference Counted）允许多个线程共享同一拥有值，最后一个 Arc 被丢弃时释放内部值。`Mutex<T>` 通过锁提供互斥可变访问；`RwLock<T>` 允许多个读者或一个写者。`Cell`/`RefCell` 主要用于单线程内部可变性，后者把借用冲突从编译期延后到运行时 panic。

项目真实实现：

- `akzio-runtime/src/runtime/store_executor.rs` 用 `Arc` 共享状态、`Semaphore` 限制并发 Store 操作，并用原子计数器记录队列和执行指标。
- `akzio-store/src/store/prelude.rs` 以 `Arc<Mutex<Connection>>` 共享唯一 SQLite 连接；连接 guard 的范围是重要的并发边界。
- `akzio-ingest/src/direct.rs` 的 `RateGate` 用 `Arc<RateGate>` 和异步 `Mutex` 保护下一次请求时间。
- `akzio-daemon/src/lib.rs`、`orchestration/bootstrap.rs` 使用 `Arc<dyn ...>` 共享 adapter/broker；trait object 必须满足其线程约束。

`Weak<T>` 用于不增加强引用计数的反向关系。本项目核心生产代码中未发现 `Rc`、`RefCell` 或 `Weak` 的主要使用；如果在测试里出现，也不能把它们当成跨线程同步方案。`Cow<'a, T>`（Copy-on-Write）在项目核心路径中未发现作为授权或持久化边界使用；它适合“多数时候借用、少数时候拥有”的值，但不能隐藏数据是否真正复制。

### 5.2 `Send`、`Sync` 与 `Pin`

`Send` 表示值可以安全地移动到另一个线程；`Sync` 表示对 `T` 的共享引用可以安全地在线程间共享。它们是编译器检查的 trait，不是锁本身。一个含有非线程安全成员的结构可能不能被 `tokio::spawn` 使用，即使外层放进 `Arc` 也不会自动变安全。

`Pin<P>` 保证被包裹的值在某些条件下不会被移动；异步 Future 可能自引用，因此运行时常把它放在 `Pin<Box<dyn Future + Send + 'a>>` 中。`Unpin` 表示值可以安全地从 Pin 中移动；没有 `Unpin` 时必须遵守 Pin API 的限制。项目运行时节点、scheduler 接口和 HTTP stream 都是理解这一点的真实入口。

## 6. Async、Future、任务、通道和取消

`async fn` 调用时先创建 Future，不会因为创建就执行；`await` 把 Future 交给当前执行器推进，直到就绪或返回错误。Tokio 的 `spawn` 把 Future 交给后台任务调度，返回 `JoinHandle`；任务何时结束、谁等待它、取消后存储什么状态，必须从调用链确认。

项目真实实现：

- `akzio-daemon/src/orchestration/` 与 `worker.rs` 通过 Tokio 任务运行调度、维护和 Outcome worker。
- `akzio-daemon/src/http.rs` 使用 `watch` 观察 shutdown、`broadcast` 向 SSE 订阅者发送事件；lagged/closed 分支不能被注释成“每个事件都保证送达”。
- `akzio-research/src/agent/runtime_run.rs` 依据统一 deadline、阶段预算和恢复状态驱动模型 Future；超时或取消会走明确的 Attempt/lease 终止路径。
- `akzio-runtime/src/runtime/store_executor.rs` 使用 `spawn_blocking` 把同步 SQLite 操作放入阻塞线程池，并用 semaphore/atomic 记录排队和完成。
- `akzio-daemon/src/scheduler/lease.rs` 和 `scheduler_core.rs` 结合 lease、oneshot、watch 处理领导权；进程内 mutex 不能被夸大为跨实例锁。

通道的语义也不同：`oneshot` 传一次结果或停止信号，`watch` 保留最近状态，`broadcast` 向多个订阅者发送但可能因缓冲不足而 lag。背压、超时、取消和任务 Join 都应在注释中写清实际边界。

特别注意跨 `await` 持锁：如果 guard 在 await 点仍活着，可能阻塞其他任务甚至形成死锁风险。本项目已有代码旁说明一些非可重入 SQLite Mutex 的边界；发现新的风险时只登记，不在本任务改写并发行为。

## 7. 宏、Serde、条件编译和工程组织

`macro_rules!` 是声明式宏，根据 token pattern 生成源码；`derive` 与 attribute macro 通常由依赖的过程宏展开。项目大量使用 `#[derive(Debug, Clone, Serialize, Deserialize, Error)]`：derive 为类型生成 trait 实现，Serde derive 决定 JSON/TOML 的序列化边界，thiserror derive 生成错误展示和 `source` 链。宏不是运行时调用，具体展开需用 `cargo expand` 或依赖源码核实，不能只看属性名称猜展开结果。

项目中未发现自定义 `macro_rules!` 或自维护过程宏 crate；实际宏主要来自 `serde`、`thiserror`、`clap`、Tokio/Axum 等依赖。`#[cfg(...)]` 与 `cfg!` 不同：前者在编译阶段裁剪代码，后者生成运行时布尔值。测试、平台分支和特性配置必须分别阅读，不能把未编译分支的静态检查当成运行验证。

Module（模块）由 `mod` 声明组织源码；`use` 引入名字；`pub`、`pub(crate)` 和私有项决定可见性。Crate 是 Cargo 的编译单元，workspace 管理多个 crate；包的 `Cargo.toml` 声明依赖、feature 和目标。修改公开 API 时要考虑 semver：改变类型、可见性或序列化字段可能影响跨 crate 调用与冻结 Contract。

## 8. 错误类型、事务和业务副作用

项目使用 `thiserror` 定义可枚举、可匹配的领域错误，使用 `anyhow` 在边界层携带上下文。应区分：

1. 输入解析失败：请求甚至没有进入业务处理。
2. Rust Contract/Validation 拒绝：状态仍可能写入审计的失败记录，但不能生成合法下游产物。
3. Store 事务失败：同一事务中的 SQLite 变更通常一起回滚，但事务外网络副作用不会自动回滚。
4. Paper API 返回 accepted：只说明外部接口受理请求，仍需查询/对账确认成交。
5. Outcome/学习不合格：可以保留数值 Outcome，但不代表 Lesson 或 Policy 被激活。

`akzio-store` 的事务 helper 和 `akzio-execution/src/paper_commitment.rs` 是阅读原子性边界的入口。先持久化 Commitment 是防重复下单的 Rust 记录；它不等于订单已发出，更不等于已成交。每个注释应标出写入前后和失败后的历史状态。

## 9. Unsafe、FFI 与未使用机制

`unsafe` 允许调用者承担编译器无法证明的安全前提，例如裸指针解引用、外部函数调用或不变量维护。`repr(C)` 控制与 C 的布局兼容，`MaybeUninit<T>` 表示内存可能尚未初始化；错误使用会造成未定义行为（UB）。Miri 可检查部分未定义行为，但它不是所有并发或外部系统问题的证明。

在当前 workspace 的业务 Rust 源码中未发现用于生产数据流的 `unsafe`、`extern "C"`、`repr(C)` 或 `MaybeUninit`；也未发现 FFI 作为 Alpaca、SQLite 或模型通道。不要为了覆盖知识清单把 unsafe 或依赖引入项目。若将来出现，应同时解释 unsafe 块前的调用方前提、块内维护的不变量、失败/释放路径和测试工具。

## 10. 性能：从可确认机制到必须测量的结论

泛型单态化可能产生多个具体实例，trait object 通过 vtable 动态分发；迭代器通常先构造惰性处理链，调用 `collect`、`for_each`、`next` 等消费者时才真正执行。`clone` 可能复制堆数据，`Arc::clone` 只增加引用计数，但二者都不能凭名字推断业务成本。

项目真实可确认点：Store Executor 用 semaphore 限制并发；SQLite 查询和 JSON 投影的过滤/排序会影响数据量；Context materialization 明确受 artifact 数和字节预算限制；SSE 使用有界 broadcast 缓冲。是否更快、是否零分配、cache 是否命中，需要 profiling、基准或 `cargo-asm` 等证据；本项目未把 `cargo-asm` 结果作为业务证明，也未发现可直接替代测量的性能结论。

## 11. 项目实际 Rust 知识索引

以下是从当前代码结构可直接回看的索引。详细注释以源码当前位置为准，覆盖登记见 `docs/ANNOTATION_COVERAGE.md`。

- 所有权、借用与 CAS：`crates/akzio-store/src/store/impl_core.rs`、`prelude.rs`、`crates/akzio-context/src/context_broker/materialization.rs`。
- 显式生命周期和 Future：`crates/akzio-context/src/broker.rs`、`crates/akzio-research/src/quality.rs`、`crates/akzio-runtime/src/runtime/node.rs`、`crates/akzio-daemon/src/scheduler.rs`。
- Trait object 与 adapter：`crates/akzio-ingest/src/adapters.rs`、`news.rs`、`runtime.rs`、`crates/akzio-daemon/src/orchestration/bootstrap.rs`。
- `Option`/`Result`/错误传播：所有 Store、Gate、adapter 和 CLI 边界；可先读 `crates/akzio-execution/src/decision_gate/decide.rs`、`crates/akzio-ingest/src/direct.rs`。
- Arc/Mutex/Semaphore/Atomic：`crates/akzio-runtime/src/runtime/store_executor.rs`、`crates/akzio-store/src/store/prelude.rs`、`crates/akzio-daemon/src/scheduler.rs`。
- async/await/取消/通道：`crates/akzio-research/src/agent/runtime_run.rs`、`crates/akzio-daemon/src/http.rs`、`crates/akzio-daemon/src/worker.rs`、`crates/akzio-daemon/src/scheduler/lease.rs`。
- 宏与序列化：`crates/akzio-domain/src/` 的 Schema 类型、`crates/akzio-model/src/lib.rs`、`crates/akzio-cli/src/main.rs` 的 Clap 类型。
- Domain 到持久化的状态流：`crates/akzio-runtime/src/runtime/workflow.rs`、`crates/akzio-store/src/store/workflow/`、`crates/akzio-execution/src/paper_commitment.rs`。

## 12. 独立教学概念清单

下列概念在本项目当前核心代码中未发现明确生产用法，仍作为 Rust 基础保留：

| 概念 | 最小认识 | 常见误区 |
| --- | --- | --- |
| HRTB `for<'a>` | 对任意生命周期都满足的约束。 | 不是把所有引用都变成 `'static`。 |
| 方差 variance | 编译器判断带生命周期类型是否可替换的规则。 | 不是运行时性能参数，也不能手写成业务配置。 |
| GAT | trait 的关联类型可以带自己的生命周期/类型参数。 | 不等于普通 associated type，也不等于泛型函数。 |
| `Rc`/`RefCell` | 单线程引用计数和运行时借用检查。 | 不能替代跨线程的 `Arc`/`Mutex`。 |
| `Weak` | 不拥有对象的弱引用，避免引用环。 | 升级失败得到 `None`，不是对象一定存在。 |
| `Cow` | 借用优先、写入时复制。 | 不保证永远不分配，且不能隐藏授权边界。 |
| `unsafe`/FFI | 把不可证明的安全前提移交给开发者。 | unsafe 块本身不是安全证明。 |
| Miri | 在受限模型下检查部分内存/别名问题。 | 不能证明网络、SQLite、Paper 或所有线程行为正确。 |
| `cargo-asm` | 查看编译后汇编以研究实现。 | 不能代替基准或业务验收。 |

## 13. 建议的阅读顺序

先从一个小的领域类型和它的 `validate` 开始，再追踪它进入 Store 的 Artifact/Ref，随后看 Context 如何筛选投影，最后看 Runtime/Daemon 如何驱动异步任务。每次遇到函数都回答五个问题：输入是谁拥有、校验在哪里发生、状态什么时候写入、外部副作用是否已经发生、返回值代表哪一个阶段。这样才能避免把编译成功、模型返回、HTTP 200、Job Completed 或 Paper accepted 误读成业务最终完成。

本指南不替代逐文件注释，也不把独立示例当作项目实现。每个文件的职责、全文阅读、函数复核、实际注释状态和排除原因应以 `docs/ANNOTATION_COVERAGE.md` 为准。
