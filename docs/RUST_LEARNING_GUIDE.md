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

### 1.3 字符串、切片、集合、迭代器和闭包

`String` 拥有一段 UTF-8 字节缓冲区；`&str` 是对 UTF-8 文本的借用视图。把 `String` 传给 `&str` 参数通常只发生自动借用，不会把字符串所有权交出去。Rust 字符串不能按“第几个字符”用整数下标访问，因为一个 Unicode 字符可能占多个字节；手工切片时索引必须落在 UTF-8 字符边界上。

项目真实实现：领域 ID 使用 `String` 保存持久化标识，而查询函数常接收 `&str`；`crates/akzio-runtime/src/runtime/replay.rs` 用 `BTreeMap` 建立可重复遍历的任务索引；`crates/akzio-learning/src/evaluation.rs` 先用迭代器适配器整理观察值，再由 `collect` 或 `try_fold` 消费。`BTreeMap` 的键序是可观察语义，不应泛化成“任何集合都有稳定顺序”。

```rust
// 独立教学示例：iter() 借用元素，filter/map 构造惰性处理链，collect() 才消费并收集结果。
let symbols = vec![String::from("TQQQ"), String::from("QQQ")];
let leveraged: Vec<&str> = symbols
    .iter()
    .map(String::as_str)
    .filter(|symbol| symbol.starts_with('T'))
    .collect();
// leveraged 中的 &str 借用 symbols 里的 String，不能比 symbols 活得更久。
```

`iter()` 产生借用元素的迭代器；`into_iter()` 是否移动元素取决于接收者的类型（对 `Vec<T>` 按值调用会消费向量并给出 `T`）；`collect()` 会把迭代器消费成目标集合。`map`、`filter` 本身只组合处理步骤，未必已经遍历数据。

闭包（closure）是能像值一样传递的小函数。它可以按共享借用、可变借用或所有权捕获外部变量；`move` 强制把捕获值移入闭包（若值是 `Copy` 则复制）。下面的 `threshold` 是整数，`move` 将它按值捕获；若闭包捕获 `String`，原绑定通常就不能再被调用方使用。`async move` 也按值捕获，但只是在创建 Future 时转移捕获值，不代表任务已被调度或执行。

```rust
// 独立教学示例：闭包拿到 threshold 的值，之后可以独立保存或交给拥有它的任务。
let threshold = 2;
let has_enough = move |items: &[i32]| items.len() >= threshold;
```

常见误区：看到 `.map(...)` 就以为转换已经发生；看到 `move` 就以为闭包自动并发；把 `&str` 当作拥有字符串；认为 `collect` 一定便宜或没有分配。实际成本由目标集合、元素类型和输入规模决定。

## 2. 所有权、Move、借用和资源释放

### 2.1 Ownership、Move、Copy、Clone

每个 Rust 值有一个所有者。把非 `Copy` 值赋给另一个变量、作为值参数传入或从函数返回，通常会 Move，原绑定不再可用；`Copy` 类型（如整数和某些小的标量）按位复制；`Clone` 是显式复制，成本和语义由实现决定，不能默认认为便宜。

项目真实实现：`Store`、运行时配置和领域 ID 经常被 `clone` 后交给异步任务或新的阶段；这表示实现需要另一个所有权句柄，并不意味着底层 SQLite、Artifact 或业务记录被复制。`DecisionGate::decide(&DecisionGateInput)` 通过共享借用读取输入，调用者仍保留输入；`&mut self` 方法则暂时独占借用接收者以修改其状态。

```rust
// 独立教学示例：String 被移动，&str 只借用。
let name = String::from("TQQQ");
let view: &str = name.as_str();
println!("{view}");
// view 最后一次使用结束后，NLL 允许再次读取 name；字符串仍由 name 持有。
println!("{name}");
```

所有权检查可以按“谁负责在最后释放这块资源”来读：值参数通常取得值的所有权，`&T` 只临时读取，`&mut T` 临时取得独占修改权限，返回值把新值或已有值的所有权交回调用方。编译器的借用检查器在编译期拒绝悬垂引用和冲突别名；它不依赖运行时引用计数。

```rust
// 独立教学示例：u32 满足 Copy，赋值复制；String 不满足 Copy，clone() 明确要求另建一份值。
let count = 3_u32;
let other_count = count;
let label = String::from("QQQ");
let copied_label = label.clone();
println!("{count} {other_count} {label} {copied_label}");
```

### 2.2 `&T`、`&mut T`、NLL、reborrow 和 partial move

`&T` 是共享借用：同一时间可以有多个读取者；`&mut T` 是独占借用：借用期间不能再有其他会冲突的读写借用。借用检查器会根据实际最后一次使用进行 NLL（Non-Lexical Lifetimes，非词法生命周期）分析，所以借用有时会在代码块结束前就结束。

reborrow（再借用）是从已有的 `&mut T` 暂时生成更短的 `&mut` 或 `&T`，让嵌套调用使用一小段独占权限，随后原来的 `&mut` 可以继续使用。partial move（部分移动）发生在从非 `Copy` 结构体中移动一个字段后：没有被移动的字段仍可能可用，但整个结构体通常不能再整体使用。

项目真实实现：`akzio-context/src/context_broker/materialization.rs` 对 `serde_json::Value` 使用 `&mut Value` 修改投影；`akzio-store` 的连接辅助类型以借用的 `MutexGuard` 持有锁保护的连接，借用范围决定何时释放锁。注释应说明“这次借用保护哪个状态、在哪里结束”，而不是只写“这里借用了”。

```rust
// 独立教学示例：&mut *counter 是更短的再借用，函数结束后原来的独占借用还可继续使用。
fn set_zero(value: &mut i32) {
    *value = 0;
}
let mut number = 4;
let counter = &mut number;
set_zero(&mut *counter);
*counter += 1;
```

```rust
// 独立教学示例：移动非 Copy 字段形成 partial move；仍可读未移动的 Copy 字段，但不能整体使用 pair。
struct Pair {
    name: String,
    count: u32,
}
let pair = Pair {
    name: String::from("TQQQ"),
    count: 1,
};
let moved_name = pair.name;
println!("{} {}", moved_name, pair.count);
```

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

项目真实实现：`crates/akzio-store/src/store/free_paper_checks.rs::parse_enum<T: for<'de> serde::Deserialize<'de>>` 使用 HRTB。`for<'de>` 要求 `T` 对任意反序列化器输入生命周期都能反序列化，所以返回值不能借用这次 SQL 字符串解析用的临时输入；函数将 SQL enum 文本包装成拥有型 JSON String，再把 JSON 解析错误转换为 `StoreError::Json`。这与固定某个 `'de` 不同。项目中未发现显式 variance（生命周期方差）声明；方差是编译器判断生命周期替换安全性的类型系统规则，不应凭直觉改写生命周期。

#### 生命周期最小示例与易混点

生命周期参数写在泛型参数位置，例如 `<'a>`；`&'a T` 表示“这个引用在 `'a` 这段有效区域内可用”。它约束的是引用之间的关系，不会让被引用对象活得更久。生命周期省略规则只是把常见关系从签名中推导出来，不会把所有引用都变成 `'static`。

```rust
// 独立教学示例：返回的切片来自 input，所以返回借用与 input 使用同一生命周期。
fn first_word<'a>(input: &'a str) -> &'a str {
    input.split_whitespace().next().unwrap_or("")
}
```

`&'static T` 是指向静态存活数据的引用；`T: 'static` 则表示 `T` 不包含比 `'static` 更短的借用（拥有型 `String` 可以满足，`&'short str` 通常不满足）。因此 `T: 'static` 不等于“把某个对象永远留在内存中”。Tokio 后台任务的 `'static` bound 是为了允许任务在调用栈返回后仍独立存在。

项目真实实现：`crates/akzio-runtime/src/runtime/node.rs` 的闭包适配实现要求 handler 返回 `Fut: Future<Output = NodeOutcome> + Send + 'static`；该 Future 由拥有型 `ClaimedAttempt` 驱动，不借用临时 `NodeContext`。方法随后将它装入带 `'_` 的 boxed Future，那个输出 Future 的借用期限仍与调用 `&self` 相关；这两个生命周期约束描述的是不同值，不能互相替代。

```rust
// 独立教学示例：T 可以是拥有型 String；含局部借用的 &str 通常不能满足 T: 'static。
fn accepts_owned_task<T: Send + 'static>(task: T) {
    drop(task);
}
accepts_owned_task(String::from("拥有的数据"));
```

HRTB（Higher-Ranked Trait Bound，高阶生命周期约束）中的 `for<'a>` 表示闭包/实现必须对任意 `'a` 都成立；它不是一个固定的长生命周期，也不等于 `'static`。项目真实的 `parse_enum` 用途见本节前文；下面再用独立教学回调示例展示 `Fn` bound 中的 `for<'a>`：

```rust
// 独立教学示例：回调必须能接受任意有效期的字符串切片。
fn measure<F>(callback: F) -> usize
where
    F: for<'a> Fn(&'a str) -> usize,
{
    callback("Akzio")
}
```

方差（variance）是编译器允许带生命周期的泛型类型进行“长借用到短借用”替换时使用的静态类型规则，不是运行时状态或性能开关。共享引用对生命周期是协变的，因此 `&'static str` 可以在需要较短 `&'a str` 的位置使用；可变引用/可变容器的替换规则更严格。本项目未声明自定义方差标记，读者不要仅凭“看起来只读”推断任意泛型容器都能缩短生命周期。

异步 trait 常把实现返回为 `Pin<Box<dyn Future<Output = T> + Send + 'a>>`：`dyn Future` 擦除了具体 Future 类型，`Box` 提供可返回的间接拥有值，`Pin` 限制 Future 被固定后移动，`+ 'a` 限制它不能借用超过所依赖对象。项目真实例子见 `crates/akzio-runtime/src/runtime/node.rs::NodeExecutor`、`crates/akzio-daemon/src/scheduler.rs` 的调度 trait 和 `crates/akzio-execution/src/paper.rs::CommittedPaperBroker`。这里的 `+ 'a` 是 Future 的类型约束，不代表借用本身有运行时计时器。

```rust
// 独立教学示例：&'static str 可以缩短为调用者要求的 &'a str；这体现共享引用的生命周期协变。
fn shorten<'a>(_: &'a (), text: &'static str) -> &'a str {
    text
}
```

## 4. Trait、泛型、约束与分发

Trait 是一组行为约束。泛型参数让一个函数或类型适用于多种具体类型；`T: Trait`、`where T: Trait` 是 trait bound（Trait 约束），要求调用者提供满足行为的类型。`impl Trait` 既可以表示“返回某个由编译器确定的具体实现”，也可以用于参数位置表达约束；`dyn Trait` 是运行时动态分发的 trait object。

项目真实实现：`akzio-ingest/src/adapters.rs` 的 `SourceDocumentFetcher`、`akzio-ingest/src/runtime.rs` 的 `AsyncEvidenceAdapter`、`akzio-runtime/src/runtime/node.rs` 的节点执行 trait，以及 `akzio-daemon/src/scheduler.rs` 的 scheduler 接口，把真实网络、fixture 和调度实现隔离开。调用方持有 `Arc<dyn AsyncEvidenceAdapter>` 或 `Pin<Box<dyn Future + Send + 'a>>` 时，只依赖 trait 公开的行为，而不依赖具体实现；代价是动态分发和对象安全约束。

Trait 默认实现写在 trait 定义的函数体中；实现类型可以直接继承，也可以覆盖。项目真实例子：`AsyncEvidenceAdapter::acquire_at` 默认委托给 `self.acquire(request)`，让未覆盖该方法的 adapter 继续提供 acquire 行为；`AgentModel::turn_with_events` 默认忽略事件 sink 并转调 `turn`，而 `capability_snapshot` 默认返回 unknown。调用方调用的是 trait 方法，运行时执行具体实现或默认实现；不要把默认值误当成 provider 已验证能力。

项目中常见的泛型形式包括 `impl Into<String>`：调用者可以传入多种可转换为 `String` 的类型，函数内部取得转换后的所有权；`Vec<T>`、`Result<T, E>` 和 `Option<T>` 则把数据类型作为参数传入容器。`where` 适合把复杂约束放到签名下方，提高阅读性。

```rust
// 独立教学示例：静态分发由调用处的具体类型确定；where 规定调用者必须提供 Display 实现。
use std::fmt::Display;

fn show<T>(value: &T) -> String
where
    T: Display,
{
    value.to_string()
}
```

`Self` 表示当前正在实现的具体类型；小写 `self` 是方法接收者，用 `self`、`&self` 或 `&mut self` 分别表达按值取得、共享借用或独占借用。Trait 的默认方法由实现者继承，也可覆盖；调用默认方法仍通过实际实现类型的 `self` 访问状态。

```rust
// 独立教学示例：Self 在默认方法中代表具体实现类型；&self 只借用对象，不把所有权拿走。
trait Named: Sized {
    fn from_label(label: String) -> Self;

    fn unnamed() -> Self {
        Self::from_label(String::from("unnamed"))
    }

    fn label(&self) -> &str;
}

struct AssetName(String);
impl Named for AssetName {
    fn from_label(label: String) -> Self {
        Self(label)
    }
    fn label(&self) -> &str {
        &self.0
    }
}
```

静态分发（static dispatch）让编译器知道具体的 `T` 并生成相应调用；动态分发（dynamic dispatch）通过 `dyn Trait` 在运行时按 vtable 选择实现。`impl Trait` 在参数位置表示“调用者可传入任何满足约束的具体类型”；在返回位置表示“返回某个具体但隐藏的类型”，同一个返回位置不能在运行时随意返回彼此不同的具体类型。

```rust
// 独立教学示例：参数位置的 impl Trait 是泛型约束；返回位置隐藏具体迭代器类型。
fn positive(values: impl IntoIterator<Item = i32>) -> impl Iterator<Item = i32> {
    values.into_iter().filter(|value| *value > 0)
}
```

Associated Type（关联类型）由 trait 的实现者为每个实现指定一个结果类型；它不同于“每次调用都能单独变化”的泛型方法。项目真实实现沿用标准库 trait 的关联类型：`Asset` 的 `TryFrom<&str>` 实现选择 `type Error = DomainError`，`ConnectionGuard` 的 `Deref` 实现选择 `type Target = Connection`，`AlpacaOptionDataFeed` 的 `FromStr` 实现选择 `type Err = String`。项目没有把关联类型作为主要业务数据模型，但 Rust 标准 trait 实现中确实使用它：

```rust
// 项目真实实现摘录：关联类型 Error 让 Result<Self, Self::Error> 在此等同 Result<Asset, DomainError>。
impl TryFrom<&str> for Asset {
    type Error = DomainError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_ascii_uppercase().as_str() {
            "TQQQ" => Ok(Self::Tqqq),
            "QQQ" => Ok(Self::Qqq),
            "SOXX" => Ok(Self::Soxx),
            "SOXL" => Ok(Self::Soxl),
            other => Err(DomainError::UnsupportedAsset(other.to_owned())),
        }
    }
}
```

```rust
// 独立教学示例：每个 Source 实现选择自己的 Item 类型。
trait Source {
    type Item;
    fn next(&mut self) -> Option<Self::Item>;
}
```

Associated Type（关联类型）在本项目的关键公开路径中不是主要建模方式。Object Safety（对象安全，较新的术语也称 dyn 兼容性）要求 trait 的方法能够在不知道具体 `Self` 大小和类型时调用。返回 `Self`、带有不受约束的泛型方法等设计可能使 trait 不能作为 `dyn Trait`；若把某方法限制为 `where Self: Sized`，该方法可以排除在 trait object 接口之外。项目中的 adapter trait 把异步结果装箱为 Future；不能只把普通泛型函数机械改成 `dyn`。

```rust
// 独立教学示例：返回 Self 的方法依赖具体实现类型，不能经 dyn Factory 调用。
trait Factory {
    fn duplicate(&self) -> Self;
}
```

GAT（Generic Associated Type，泛型关联类型）允许关联类型本身带生命周期或类型参数。当前项目未发现 GAT 的实际使用；独立示例：

```rust
// 独立教学示例：关联类型的生命周期参数描述由借用 self 得到的视图期限。
trait Windowed {
    type Window<'a>
    where
        Self: 'a;

    fn window(&self) -> Self::Window<'_>;
}
```

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

本仓库另外用 `std::cell::Cell<bool>` 在 `crates/akzio-store/src/store/impl_core.rs` 的线程局部标记中检测同一线程是否重复进入连接 guard；它不是跨线程共享锁。源码中未发现业务 Rust 使用 `RwLock`、`Rc`、`RefCell`、`Weak` 或 `Cow`。下面这些是独立教学示例，说明指针选择会改变谁拥有值、是否能跨线程共享以及可变性在哪里检查：

```rust
// 独立教学示例：Box 唯一拥有堆上值；Cell 在单线程通过 get/set 修改 Copy 值。
use std::cell::Cell;

let boxed = Box::new(String::from("owned"));
let counter = Cell::new(0_u32);
counter.set(counter.get() + 1);
drop(boxed);
```

```rust
// 独立教学示例：RefCell 将借用冲突延迟到运行时；Rc/Weak 是单线程共享/非拥有引用。
use std::{cell::RefCell, rc::Rc};

let owner = Rc::new(RefCell::new(String::from("shared")));
let observer = Rc::downgrade(&owner);
owner.borrow_mut().push_str(" state");
assert!(observer.upgrade().is_some());
drop(owner);
assert!(observer.upgrade().is_none());
```

```rust
// 独立教学示例：Cow 可以先借用，into_owned 在需要拥有值时取得 String。
use std::borrow::Cow;

let text: Cow<'_, str> = Cow::Borrowed("read-only");
let owned: String = text.into_owned();
```

`RwLock<T>` 用读锁允许多个并发读者、写锁允许单个写者；它仍是锁，锁 guard 离开作用域才释放。项目没有以它管理状态，不能因为 `RwLock` 有“读多写少”的名字就推断它更适合某段业务。`Box<T>` 只表达唯一所有权，不等于自动固定地址；真正的固定语义来自 `Pin<P>`。

```rust
// 独立教学示例：Arc 只共享一个 Mutex 容器；MutexGuard 在花括号结束时解锁。
use std::sync::{Arc, Mutex};

let values = Arc::new(Mutex::new(vec![1]));
let worker_handle = Arc::clone(&values);
{
    let mut locked = worker_handle.lock().expect("lock is not poisoned");
    locked.push(2);
}
// 锁已经释放；values 仍是拥有共享状态的 Arc 句柄。
```

```rust
// 独立教学示例：RwLock 读 guard 与写 guard 的存活范围分别限制后续读写。
use std::sync::RwLock;

let balance = RwLock::new(10_i64);
{
    let reader = balance.read().expect("read lock is not poisoned");
    assert_eq!(*reader, 10);
}
{
    let mut writer = balance.write().expect("write lock is not poisoned");
    *writer += 5;
}
```

### 5.2 `Send`、`Sync` 与 `Pin`

`Send` 表示值可以安全地移动到另一个线程；`Sync` 表示对 `T` 的共享引用可以安全地在线程间共享。它们是编译器检查的 trait，不是锁本身。一个含有非线程安全成员的结构可能不能被 `tokio::spawn` 使用，即使外层放进 `Arc` 也不会自动变安全。

项目真实实现：`crates/akzio-daemon/src/worker.rs::TaskHandler` 要求 handler 闭包与其 Future 满足 `Send`/`Sync`，因为 Worker 会在 Tokio runtime 中调度它；`crates/akzio-runtime/src/runtime/store_executor.rs` 把并发状态放在 `Arc` 中，并将计数指标用 `Atomic*` 管理。编译器根据字段类型递归判断这些 trait，`Arc` 不会把一个本身非 `Send`/`Sync` 的成员变安全。

```rust
// 独立教学示例：这些函数只在编译期要求类型满足自动 trait，不会启动线程。
fn require_send<T: Send>() {}
fn require_sync<T: Sync>() {}

require_send::<String>();
require_sync::<String>();
```

```rust
// 独立教学示例：spawn 的闭包和值会被移动进线程，所以 T 需要 Send + 'static。
fn return_from_thread<T: Send + 'static>(value: T) -> std::thread::JoinHandle<T> {
    std::thread::spawn(move || value)
}
```

`Pin<P>` 保证被包裹的值在某些条件下不会被移动；异步 Future 可能自引用，因此运行时常把它放在 `Pin<Box<dyn Future + Send + 'a>>` 中。`Unpin` 表示值可以安全地从 Pin 中移动；没有 `Unpin` 时必须遵守 Pin API 的限制。项目运行时节点、scheduler 接口和 HTTP stream 都是理解这一点的真实入口。

`Pin` 不是“永不释放”：它限制被固定后的移动方式，所有者离开作用域时值仍会正常 Drop。`Unpin` 则是一个 trait，表示类型不依赖固定地址，可以安全地从 pin 投影中移动；大多数普通值实现 `Unpin`。本项目显式用 `Pin<Box<dyn Future + Send + 'a>>` 擦除不同异步实现的具体类型；没有发现业务代码手写 `Waker` 或自定义 `Future::poll`。

```rust
// 独立教学示例：Future::poll 接收 Pin<&mut Self>，Pending 时可请求执行器稍后再次 poll。
use std::{future::Future, pin::Pin, task::{Context, Poll}};

fn poll_once<F: Future>(future: Pin<&mut F>, context: &mut Context<'_>) -> Poll<F::Output> {
    future.poll(context)
}
```

```rust
// 独立教学示例：仅展示如何从 Context 取出 Waker 并请求再次调度，不代表 Akzio 自行实现执行器。
fn signal_progress(context: &std::task::Context<'_>) {
    context.waker().wake_by_ref();
}
```

`Unpin` 的可观察效果可以从安全 API 看：对 `T: Unpin`，`Pin<&mut T>::get_mut()` 能安全返回普通 `&mut T`；对 `!Unpin`，不能这样解除固定。以下仅展示 bound，不代表项目业务 Future 都是 `!Unpin`：

```rust
// 独立教学示例：只有实现 Unpin 的 T 才能经 get_mut 取回普通可变引用。
fn edit_unpinned<T: Unpin>(mut value: Pin<&mut T>) -> &mut T {
    value.as_mut().get_mut()
}
```

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

### 6.1 线程、任务、Waker 与取消边界

OS 线程（thread）是操作系统调度的执行载体；Tokio task 是由 Tokio runtime 在工作线程上轮询的异步计算，不等同于“一项 task 固定占一个线程”。`tokio::spawn` 把一个满足线程约束的 Future 交给 runtime；`spawn_blocking` 把阻塞闭包放到专用阻塞线程池。项目用后者运行同步 SQLite 工作，再由 `StoreExecutor` 的 semaphore 限制同时进入的 Store 操作；是否真的并行还受 permit、线程池和锁影响，不能只从 `async` 关键字推断。

Future 通常由执行器调用 `poll` 推进；当 Future 暂时不能继续时可返回 `Poll::Pending`。`Waker` 是 Future 通知执行器“条件可能已变化、可以再 poll”的句柄。Akzio 调用 Tokio/Axum 提供的执行器和通道，没有自定义 `Waker` 实现，因此不要在应用注释中虚构自定义调度策略。

项目中不同通道含义不同：`oneshot` 传一次结果或停止信号；`watch` 保存并通知最新状态（例如 shutdown 标志）；`broadcast` 把事件复制给多个订阅者，但缓冲溢出时接收者会得到 `Lagged`；测试里的 `std::sync::mpsc` 负责跨线程同步。SSE 观察到 `Lagged` 后不能表述为每个事件都送达。`Semaphore` 提供并发上限而非无界队列；排队、超时和取消还要看它的 permit 在哪个作用域释放。

取消的真实含义取决于被取消的层。`TaskRuntime::run_one_for_workload` 在 handler、心跳/取消监控与 wall-time Future 之间做选择，离开选择块后先 drop 子 Future，再提交终态；Durable cancel 标志由 worker 在心跳间隔观察。若同步数据库闭包已交给 `spawn_blocking`，丢弃等待该闭包的异步 Future 不应被误写为数据库工作已经回滚。查看 `crates/akzio-runtime/src/runtime/task.rs` 与 `store_executor.rs` 的注释时，分别确认 Future 取消、Store 完成、permit 释放与 lease 终态。

```rust
// 独立教学示例：调用 async 函数先得到 Future；只有在另一个 async 上下文 await 才会推进它。
async fn fetch_number() -> Result<u32, &'static str> {
    Ok(7)
}

async fn caller() -> Result<u32, &'static str> {
    let pending_future = fetch_number(); // 创建 Future，不是后台任务
    let value = pending_future.await?;   // 当前任务把 Future 交给执行器轮询
    Ok(value)
}
```

```rust
// 独立教学示例：spawn 将 Future 交给 Tokio runtime；JoinHandle.await 等待任务结束并取得其结果。
async fn launch() -> Result<u32, tokio::task::JoinError> {
    let handle = tokio::spawn(async move { 7_u32 });
    handle.await
}
```

有界通道（bounded channel）在队列装满时可以把发送方挂起，形成背压（backpressure），避免生产者无限堆积待处理消息。项目的 worker 主要通过 semaphore 限制可同时执行的 Store 工作，HTTP 事件使用有限容量的 `broadcast`；以下 `mpsc` 仅为独立教学示例：

```rust
// 独立教学示例：容量为 1 时，send 只有在 receiver 存在且有缓冲空间时才能完成。
async fn bounded_message() -> Result<(Option<u32>, Option<u32>), tokio::task::JoinError> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    sender.send(10).await.expect("receiver still exists");
    // 队列已满；这个后台发送 Future 会等到下面的 recv 腾出容量。
    let sender_task = tokio::spawn(async move {
        sender.send(20).await.expect("receiver still exists");
    });
    let first = receiver.recv().await;
    sender_task.await?;
    let second = receiver.recv().await;
    Ok((first, second))
}
```

```rust
// 独立教学示例：AtomicUsize 原子更新单个计数；Relaxed 不为其他数据建立同步关系。
use std::sync::atomic::{AtomicUsize, Ordering};

let counter = AtomicUsize::new(0);
counter.fetch_add(1, Ordering::Relaxed);
```

`tokio::time::timeout(duration, future).await` 外层 `Err` 表示等待超时，内层 Future 自己仍可能返回 `Result::Err`；要区分两层错误。超时选择会让调用方不再等待，不等价于数据库事务回滚或远端请求必然停止。项目在 `crates/akzio-runtime/src/runtime/task.rs` 将 wall-time 到期转成 `Retry(Timeout)`，之后仍通过 Store 结束当前 Attempt。

## 7. 宏、Serde、条件编译和工程组织

`macro_rules!` 是声明式宏，根据 token pattern 生成源码；`derive` 与 attribute macro 通常由依赖的过程宏展开。项目大量使用 `#[derive(Debug, Clone, Serialize, Deserialize, Error)]`：derive 为类型生成 trait 实现，Serde derive 决定 JSON/TOML 的序列化边界，thiserror derive 生成错误展示和 `source` 链。宏不是运行时调用，具体展开需用 `cargo expand` 或依赖源码核实，不能只看属性名称猜展开结果。

项目真实实现：`crates/akzio-domain/src/lib.rs::id_type!` 是自维护的声明式宏。它匹配一个 `$name:ident` 标识符；例如 `crates/akzio-domain/src/core.rs` 和 `ids.rs` 中的 `id_type!(RunId)`、`id_type!(OutcomeId)` 会在编译期展开成透明 `String` 新类型，以及统一的 `new`、`Default`、`Display` 和 Serde 派生。理解宏时先看 matcher 接受什么，再读展开模板；宏调用处看起来短，不代表只生成一个字段。此处宏模板可直接检查，但本次没有运行 `cargo expand`。

项目也大量使用依赖提供的 procedural macro（过程宏）：`#[derive(Serialize, Deserialize)]`/`#[derive(Error)]` 会按类型生成 trait 实现；CLI 的 Clap derive 会按参数类型生成解析入口。`#[serde(...)]` 是配套 derive 读取的 helper attribute，用来配置字段/枚举序列化，不等于本仓库自写了 `#[proc_macro_attribute]`。本 workspace 没有自维护过程宏 crate，也未发现自维护 attribute procedural macro。展开结果应看宏定义/官方依赖源码，或在有合适工具时用 `cargo expand`；此处没有运行 expand，也不根据属性名猜生成代码。

还有三种源码嵌入宏要分清：`include!("x.rs")` 在编译时把另一个 Rust 源文件的 tokens 放到调用位置；`include_str!("x.md")` 把 UTF-8 文件作为 `&'static str` 编入程序；`include_bytes!("x")` 把原始文件 bytes 编成静态字节片。项目例子：`crates/akzio-research/src/agent.rs` 用 `include!` 组合实现模块；`crates/akzio-ingest/src/news.rs` 以 `include_str!` 读取 source-verifier Prompt；`crates/akzio-research/src/prompt_registry.rs` 用 `include_bytes!` 登记 Prompt 及源码身份输入。对 `include_bytes!` 覆盖的文件加注释也会改变字节哈希，因此这些路径要先核对身份消费方，不能按普通文本直接改。

`#[cfg(...)]` 会在编译阶段选择要保留的代码；例如 `crates/akzio-cli/src/cli/run_commands.rs` 按 Unix 与非 Unix 编译不同信号处理实现，`#[cfg(test)]` 只把测试模块加入测试构建。`cfg!(...)` 则是编译时计算后产生普通布尔值，分支两边仍都要通过类型检查。项目主要使用的是 `#[cfg]`，不要把“当前平台未编译”解释成“代码已经运行验证”。

Module（模块）由 `mod` 声明组织源码；`use` 引入名字；`pub`、`pub(crate)` 和私有项决定可见性。Crate 是 Cargo 的编译单元，workspace 管理多个 crate；包的 `Cargo.toml` 声明依赖、feature 和目标。修改公开 API 时要考虑 semver：改变类型、可见性或序列化字段可能影响跨 crate 调用与冻结 Contract。

工程定位例子：根 `Cargo.toml` 的 `[workspace].members` 把 11 个 crate 放进同一 Cargo workspace；`crates/akzio-domain/src/lib.rs` 用 `pub mod` 与 `pub use` 组织 crate 对外类型；各 crate 的 `Cargo.toml` 通过 `akzio-domain = { path = ... }` 建立本地依赖。`publish = false` 表示这些包不发布到 crates.io，不会消除它们之间的 Rust API 边界；冻结 Contract、序列化字段与 CAS 哈希可能比通常的 semver 兼容承诺更敏感。

```rust
// 项目真实实现：模块声明参与编译，pub use 再把模块中的类型导出到 crate 根路径。
pub mod budget;
pub use budget::{AgentBudgetConfig, AgentSettings};
```

Cargo feature 是编译配置选项，能开启依赖功能或 crate 自身 `#[cfg(feature = "...")]` 路径；根 manifest 当前显式为 workspace 依赖选择了 reqwest、tokio 等依赖 feature，但各业务 crate 没有自定义 `[features]` 表。是否进入某个功能分支要同时检查 manifest 的 feature 声明、命令行选择及 `#[cfg]` 条件，不能把 Cargo feature 与运行时配置值混为一谈。

```toml
# 独立教学示例：feature 是编译配置名；没有启用 fast-mode 时该 crate 不会编译对应函数。
[features]
fast-mode = []
```

```rust
// 独立教学示例：feature 条件在编译期裁剪项；平台 cfg 也是编译期选择，不是运行时 if。
#[cfg(feature = "fast-mode")]
fn optimized_path() {}

#[cfg(unix)]
fn platform_path() {}
```

项目真实平台条件见 `crates/akzio-cli/src/cli/run_commands.rs` 的 Unix/non-Unix 分支；根 workspace 自身固定 `edition = "2021"`、`rust-version = "1.96"`，共用依赖版本由根 `[workspace.dependencies]` 管理。修改一个 `pub` 函数签名、公开 enum 变体或 Serde 字段可能影响其他 crate、下游构造代码、冻结 Contract 和序列化身份；semver 检查不能只看 Cargo 包是否发布。

## 8. 错误类型、事务和业务副作用

项目使用 `thiserror` 定义可枚举、可匹配的领域错误，使用 `anyhow` 在边界层携带上下文。应区分：

1. 输入解析失败：请求甚至没有进入业务处理。
2. Rust Contract/Validation 拒绝：状态仍可能写入审计的失败记录，但不能生成合法下游产物。
3. Store 事务失败：同一事务中的 SQLite 变更通常一起回滚，但事务外网络副作用不会自动回滚。
4. Paper API 返回 accepted：只说明外部接口受理请求，仍需查询/对账确认成交。
5. Outcome/学习不合格：可以保留数值 Outcome，但不代表 Lesson 或 Policy 被激活。

`akzio-store` 的事务 helper 和 `akzio-execution/src/paper_commitment.rs` 是阅读原子性边界的入口。先持久化 Commitment 是防重复下单的 Rust 记录；它不等于订单已发出，更不等于已成交。每个注释应标出写入前后和失败后的历史状态。

身份哈希的源码路径也是可观察输入：`crates/akzio-research/src/lib.rs` 将登记的 path 与 source bytes 交错后计算 Prompt/Contract identity，`crates/akzio-runtime/src/lib.rs` 对冻结 workflow/topology 组件做同类计算；CLI `build.rs` 另用 Git HEAD 与工作树 diff 建立 `code_revision`。因此普通注释不会改变 Rust 表达式的执行结果，却可能改变身份哈希并令旧 runtime identity/permit 无法沿用。历史 Store CAS、Contract 或 Commitment 不会被这些注释自动重写；hash-sensitive 源码的注释限制和外部导读见 `docs/ANNOTATION_COVERAGE.md`。

`thiserror` 适合定义能被调用方按变体匹配的库/领域错误；`#[from]` 可以生成受控的错误转换。`anyhow` 适合应用边界把多种底层错误带着上下文向上返回，不应把它当作领域 Contract。项目真实实现可看 `crates/akzio-domain/src/core.rs`、各 crate 的 `Error` enum，以及 `crates/akzio-cli/src/main.rs` 的应用错误边界。

```rust
// 独立教学示例：thiserror 保留可匹配的具体错误；anyhow::Context 给底层错误补上操作背景。
#[derive(Debug, thiserror::Error)]
enum ConfigError {
    #[error("配置缺少字段：{0}")]
    Missing(String),
}

fn read_config(path: &std::path::Path) -> anyhow::Result<String> {
    use anyhow::Context;
    std::fs::read_to_string(path)
        .with_context(|| format!("读取配置 {}", path.display()))
}
```

`panic!`、`unwrap()` 与 `expect()` 会走 panic 路径，而不是返回 `Err` 给当前调用者；具体进程/线程的最终处置还取决于运行环境与 panic 配置。它们适合测试中让失败立即显现，或在已经由前置校验建立的不变量处表达程序员错误，不适合把外部输入、网络响应或可缺失的数据库行当成必然存在。`expect("...")` 的文字只帮助定位 panic，不会验证前置条件。遇到 `Result`/`Option`，先看失败是否应局限在一条记录，再选择传播、转成状态还是拒绝整个请求。

```rust
// 独立教学示例：可缺失输入应返回错误，而不是让调用者承受 panic。
fn required_name(value: Option<&str>) -> Result<&str, ConfigError> {
    value.ok_or_else(|| ConfigError::Missing("name".to_owned()))
}
```

```rust
// 独立教学示例：unwrap/expect 在 None 时 panic；只适合已经建立不变量的边界或测试。
let optional_name: Option<&str> = None;
let _name = optional_name.expect("前置检查保证此值存在"); // 此例会在这里 panic，用于说明失败边界。
```

## 9. Unsafe、FFI 与未使用机制

`unsafe` 允许调用者承担编译器无法证明的安全前提，例如裸指针解引用、外部函数调用或不变量维护。`repr(C)` 控制与 C 的布局兼容，`MaybeUninit<T>` 表示内存可能尚未初始化；错误使用会造成未定义行为（UB）。Miri 可检查部分未定义行为，但它不是所有并发或外部系统问题的证明。

在当前 workspace 的业务 Rust 源码中未发现用于生产数据流的 `unsafe`、`extern "C"`、`repr(C)` 或 `MaybeUninit`；也未发现 FFI 作为 Alpaca、SQLite 或模型通道。不要为了覆盖知识清单把 unsafe 或依赖引入项目。若将来出现，应同时解释 unsafe 块前的调用方前提、块内维护的不变量、失败/释放路径和测试工具。

`unsafe fn` 的实现方必须明确记录它依赖的安全前提；调用方只有在满足这些前提时才可在 `unsafe` 调用点承担责任。unsafe block 不会自动把坏指针变安全，unsafe trait 的实现者则需要满足 trait 声明的不变量，使用者依赖这些承诺。项目当前没有这些业务边界，因此下例只展示调用侧的一个具体前提：

下面仅为独立教学示例。裸指针 `*const T`/`*mut T` 不带 Rust 引用的借用保证；解引用必须处于 `unsafe` 中，并由调用者保证指针有效、对齐、初始化且访问时没有违反别名/线程规则。`unsafe` 只是把编译器无法验证的前提明确交给程序员，不会自动让操作安全：

```rust
// 独立教学示例：value 在整个解引用期间仍有效且对齐，因此这个只读解引用满足前提。
let value = 7_i32;
let pointer: *const i32 = &value;
let copied = unsafe { *pointer };
assert_eq!(copied, 7);
```

`MaybeUninit<T>` 允许先拥有未初始化存储；只有在每条执行路径都已写入有效 `T` 后才能调用 `assume_init`。`#[repr(C)]` 请求按 C ABI 兼容规则布局结构体；`extern "C"` 声明外部 ABI 函数，调用方还需满足指针、线程和生命周期等 C 合约。以下仍是独立示例，不是项目 FFI：

```rust
// 独立教学示例：写入完整值后才 assume_init；若漏掉 write，后续读取会造成未定义行为。
let mut slot = std::mem::MaybeUninit::<u32>::uninit();
slot.write(42);
let initialized = unsafe { slot.assume_init() };
assert_eq!(initialized, 42);
```

```rust
// 独立教学示例：repr(C) 约束字段布局；外部符号及其调用安全前提需由 FFI 双方共同满足。
#[repr(C)]
struct CPoint {
    x: std::os::raw::c_int,
    y: std::os::raw::c_int,
}

unsafe extern "C" {
    fn abs(value: std::os::raw::c_int) -> std::os::raw::c_int;
}
// 这里仅展示调用语法；示例需在提供 C 标准库 abs 符号的链接环境中才能运行。
let magnitude = unsafe { abs(-3) };
```

Miri 是一种解释执行/动态检查工具，可发现部分未定义行为和别名违规，但不能证明所有输入、并发时序、网络协议或外部 C 库正确。当前仓库没有声明 Miri 验收任务，本次也没有运行它；在独立学习 crate 中常见的命令形式是 `cargo +nightly miri test`，这要求对应 nightly/Miri 工具链，不等同于仓库固定 Rust `1.96.0` 的标准检查。

## 10. 性能：从可确认机制到必须测量的结论

泛型单态化可能产生多个具体实例，trait object 通过 vtable 动态分发；迭代器通常先构造惰性处理链，调用 `collect`、`for_each`、`next` 等消费者时才真正执行。`clone` 可能复制堆数据，`Arc::clone` 只增加引用计数，但二者都不能凭名字推断业务成本。

项目真实可确认点：Store Executor 用 semaphore 限制并发；SQLite 查询和 JSON 投影的过滤/排序会影响数据量；Context materialization 明确受 artifact 数和字节预算限制；SSE 使用有界 broadcast 缓冲。是否更快、是否零分配、cache 是否命中，需要 profiling、基准或 `cargo-asm` 等证据；本项目未把 `cargo-asm` 结果作为业务证明，也未发现可直接替代测量的性能结论。

零成本抽象（zero-cost abstraction）是 Rust 的设计目标之一：抽象层在可优化时不应带来比手写等价实现更多的运行期开销；这不是“所有 Rust 代码无分配、无分支、一定更快”的承诺。泛型可能因单态化增加机器码体积；`dyn Trait` 有 vtable 间接调用；`Box`/`Vec` 可能分配；`Arc::clone` 不复制内部值但会更新引用计数；普通 `String::clone` 则通常会复制缓冲区内容。需要检查特定结论时，可看优化后的汇编（例如 `cargo-asm`）并配合基准测试，而不能只看源代码语法。

内存布局也有边界：普通 `struct` 的字段顺序与填充不应被当作稳定 ABI；Akzio 的 `id_type!` 对透明新类型使用 `#[repr(transparent)]`，这是显式布局承诺，不能据此推断所有领域结构体同样布局。CPU cache（缓存）局部性取决于访问顺序、数据布局、编译器与硬件；源码中有 `Vec` 或有序 `BTreeMap` 并不能证明命中率或性能优势。

```text
独立工具示例（不代表仓库已安装或已运行）：
cargo asm -p akzio-runtime path::to::function
```

本项目性能上可确认的事实仅包括：`StoreExecutor` semaphore 的并发上限、集合的实际排序/去重语义、查询的过滤范围、SSE 缓冲上限与 Context 的显式 Artifact/字节预算。是否零分配、是否命中缓存、哪种分发更快及某次优化是否有效，都需要在目标构建与代表性输入上测量；本任务不引入 benchmark 依赖，也不据注释推断速度。

```rust
// 独立教学示例：顺序扫描 Vec 暴露连续遍历的数据访问方式；是否更适合目标 CPU cache 仍需测量。
let values = vec![1_u64, 2, 3, 4];
let sum = values.iter().copied().sum::<u64>();
assert_eq!(sum, 10);
```

“cache 友好”是对访问模式与具体机器的性能假设，不是 Rust 类型的功能保证；项目没有以 CPU cache 数据作为校准或业务正确性证据。

## 11. 项目实际 Rust 知识索引

以下是从当前代码结构可直接回看的索引。详细注释以源码当前位置为准，覆盖登记见 `docs/ANNOTATION_COVERAGE.md`。

- 所有权、借用与 CAS：`crates/akzio-store/src/store/impl_core.rs`、`prelude.rs`、`crates/akzio-context/src/context_broker/materialization.rs`。
- 显式生命周期和 Future：`crates/akzio-context/src/broker.rs`、`crates/akzio-research/src/quality.rs`、`crates/akzio-runtime/src/runtime/node.rs`、`crates/akzio-daemon/src/scheduler.rs`。
- Trait object 与 adapter：`crates/akzio-ingest/src/adapters.rs`、`news.rs`、`runtime.rs`、`crates/akzio-daemon/src/orchestration/bootstrap.rs`。
- `Option`/`Result`/错误传播：所有 Store、Gate、adapter 和 CLI 边界；可先读 `crates/akzio-execution/src/decision_gate/decide.rs`、`crates/akzio-ingest/src/direct.rs`。
- HRTB 与 Associated Type：`crates/akzio-store/src/store/free_paper_checks.rs::parse_enum` 的 `for<'de> Deserialize<'de>`；`crates/akzio-domain/src/core.rs::TryFrom<&str> for Asset` 的 `Error`、`crates/akzio-store/src/store/impl_core.rs::Deref` 的 `Target`。
- Arc/Mutex/Semaphore/Atomic：`crates/akzio-runtime/src/runtime/store_executor.rs`、`crates/akzio-store/src/store/prelude.rs`、`crates/akzio-daemon/src/scheduler.rs`。
- async/await/取消/通道：`crates/akzio-research/src/agent/runtime_run.rs`、`crates/akzio-daemon/src/http.rs`、`crates/akzio-daemon/src/worker.rs`、`crates/akzio-daemon/src/scheduler/lease.rs`。
- 宏与序列化：`crates/akzio-domain/src/` 的 Schema 类型、`crates/akzio-model/src/lib.rs`、`crates/akzio-cli/src/main.rs` 的 Clap 类型。
- `macro_rules!` matcher、生成的新类型和透明布局：`crates/akzio-domain/src/lib.rs::id_type!` 及其在 `crates/akzio-domain/src/core.rs`、`ids.rs` 的调用。
- 迭代器惰性、闭包捕获与有序集合：`crates/akzio-runtime/src/runtime/replay.rs`、`crates/akzio-learning/src/evaluation.rs`、`crates/akzio-daemon/src/observer_analytics.rs`。
- `Cell`、Arc/Mutex/Atomic/Pin：`crates/akzio-store/src/store/impl_core.rs`、`crates/akzio-store/src/store/prelude.rs`、`crates/akzio-runtime/src/runtime/store_executor.rs`、`crates/akzio-runtime/src/runtime/node.rs`。
- Domain 到持久化的状态流：`crates/akzio-runtime/src/runtime/workflow.rs`、`crates/akzio-store/src/store/workflow/`、`crates/akzio-execution/src/paper_commitment.rs`。

## 12. 独立教学概念清单

下表列出本次检索当前 workspace 源码后未发现“业务实现主动使用”的机制。每项的独立示例位于对应章节；“未发现”不表示 Rust 不支持，也不表示依赖内部绝无实现，而是指本仓库维护的业务 Rust 源文件没有直接采用该机制。HRTB 虽然常见于教学示例，本项目确有一个真实用途，详见 §3 与 `free_paper_checks.rs::parse_enum`。项目中实际使用的机制请看上一节索引与各章“项目真实实现”段落。

| 概念 | 本项目状态与最小示例位置 | 解决的问题 | 常见误区 |
| --- | --- | --- | --- |
| 显式 variance 标注 | **项目中未发现实际使用**；生命周期缩短示例见 §3。 | 编译器判断泛型中的生命周期能否安全替换。 | 不是运行时性能参数；不能因为字段只读就推断所有容器协变。 |
| GAT | **项目中未发现实际使用**；独立示例见 §4。 | 让 trait 关联类型本身携带生命周期或类型参数。 | 不等于普通关联类型，也不等于泛型方法。 |
| `Rc` / `RefCell` | **业务代码中未发现实际使用**；独立示例见 §5.1。 | 单线程引用计数及运行时借用检查。 | 不能替代跨线程 `Arc`/`Mutex`；冲突借用会 panic。 |
| `Weak` | **项目中未发现实际使用**；与 `Rc` 的独立示例见 §5.1。 | 不增加强引用计数地观察对象，常用于打断引用环。 | `upgrade()` 返回 `None` 是正常情况，对象可能已经释放。 |
| `Cow` | **项目中未发现实际使用**；独立示例见 §5.1。 | 先借用、需要拥有时才转换/复制。 | 不保证永不分配，也不能模糊数据来源或授权边界。 |
| `RwLock` | **项目中未发现实际使用**；独立示例见 §5.1。 | 多读者或单写者的锁式共享。 | 读写比例看似合适不等于实际性能更好；guard 仍需及时释放。 |
| 显式 `Unpin` bound | **项目接口未发现显式约束**；`Pin`/`Unpin` 示例见 §5.2。 | 表示 pinned 值可安全从固定位置移动。 | `Pin` 不阻止 Drop，也不代表被指对象永不释放。 |
| 自定义 `Future::poll` / `Waker` | **项目调用 Tokio/Axum 实现，未发现自维护实现**；独立示例见 §5.2、§6.1。 | 执行器与 Future 之间的就绪通知。 | `Waker` 不是线程或任务本身；重复唤醒不保证业务条件已满足。 |
| `unsafe`、裸指针、`MaybeUninit` | **业务 Rust 中未发现实际使用**；安全边界示例见 §9。 | 表达编译器无法验证的内存/别名前提。 | `unsafe` 块不是安全证明；前置条件失败可能触发 UB。 |
| `extern "C"` / `repr(C)` | **业务 Rust 中未发现实际使用**；FFI 示例见 §9。 | 与 C ABI 交互及约定跨语言布局。 | 仅有 `repr(C)` 不会自动保证指针有效或 C 函数语义正确。 |
| Miri | **本项目未设置 Miri 验收**；工具说明见 §9。 | 动态检查部分未定义行为与别名问题。 | 不能证明所有并发、网络、Paper 或外部库正确。 |
| `cargo-asm` | **本项目未发现测量记录**；命令示例见 §10。 | 查看指定函数的编译后汇编。 | 不能代替基准、profile 或业务验收。 |
| `cfg!` 宏 | **维护源码中未发现调用**；项目实际 `#[cfg]` 与独立 feature 示例见 §7。 | `cfg!` 生成布尔值，适合运行时条件选择。 | 它不会像 `#[cfg]` 那样从编译单元裁掉分支。 |
| 自维护过程宏 crate | **workspace 未发现 proc-macro crate**；宏分类见 §7。 | 在编译阶段解析/生成 Rust 代码。 | 依赖的 derive macro 不等于仓库自维护过程宏；未展开时不要臆测生成实现。 |

项目实际 HRTB 的逐项阅读记录：`parse_enum` 的参数 `value: &str` 仅借给 JSON String 构造过程；`T` 满足对任意 `'de` 的 `Deserialize<'de>`，因此调用方得到不借用该输入的 `T`。它把 SQL 状态文本解码为拥有型领域 enum；serde 解析失败经 `map_err(StoreError::Json)` 返回给当前调用者，而不是 panic。这个解释只覆盖该解析 helper 的 HRTB 用途，不表示整 crate 采用 HRTB 做回调调度。

## 13. 建议的阅读顺序

先从一个小的领域类型和它的 `validate` 开始，再追踪它进入 Store 的 Artifact/Ref，随后看 Context 如何筛选投影，最后看 Runtime/Daemon 如何驱动异步任务。每次遇到函数都回答五个问题：输入是谁拥有、校验在哪里发生、状态什么时候写入、外部副作用是否已经发生、返回值代表哪一个阶段。这样才能避免把编译成功、模型返回、HTTP 200、Job Completed 或 Paper accepted 误读成业务最终完成。

本指南不替代逐文件注释，也不把独立示例当作项目实现。每个文件的职责、全文阅读、函数复核、实际注释状态和排除原因应以 `docs/ANNOTATION_COVERAGE.md` 为准。
