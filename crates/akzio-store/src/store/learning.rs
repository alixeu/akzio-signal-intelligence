// 文件导读：学习子模块把 Outcome/Retrospective、Policy transition、Shadow pair
// 组织成同一套持久化边界；每个 include 文件只补充 Store impl，不改变学习资格的权威来源。
// `include!` 是编译期展开源码片段而非运行时加载；文件内容共享本模块的 `use super::*`。
// 建议先读 outcome 的阶段与 lease
// 写入，再读 policy 的资格/CAS 提交，最后看 shadow/history 的比较记录和只读历史查询。
use super::*;
include!("learning/outcome.rs");
include!("learning/policy.rs");
include!("learning/shadow.rs");
include!("learning/history.rs");
