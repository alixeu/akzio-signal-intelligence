// 文件导读：导出运行时 Agent 唯一可用的 Manifest/Grant Context 沙箱接口。
// 具体实现隐藏在 broker 模块中，调用方不能绕过授权直接读取 Store 内容。
//! The only context route available to agent tasks.
//!
//! The root surface is manifest-and-grant based.

mod broker;

pub use crate::broker::{
    ContextBroker, ContextDocumentMetadata, ContextError, ContextManifest, ContextMaterialization,
    ContextMustReadDocument, ContextReadResult, ContextResult as Result,
};
