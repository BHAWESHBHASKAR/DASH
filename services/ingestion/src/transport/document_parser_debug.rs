use std::{path::Path, process::Command};

use super::config::env_with_fallback;
use super::json::json_escape;

const DEFAULT_DOCUMENT_PARSER_PROVIDER: &str = "builtin_utf8";
const DEFAULT_EMBEDDING_PROVIDER: &str = "hash_vector";
const DEFAULT_EMBEDDING_DIMENSIONS: usize = 64;
const DOCUMENT_ADAPTER_SCRIPT_PATH: &str = "scripts/document_extract_adapter.sh";

const BUILTIN_UTF8_MIME_TYPES: [&str; 9] = [
    "text/*",
    "application/json",
    "application/xml",
    "text/xml",
    "application/yaml",
    "application/x-yaml",
    "application/markdown",
    "text/markdown",
    "application/csv",
];

const TOOL_NAMES: [&str; 10] = [
    "pdftotext",
    "mutool",
    "pandoc",
    "docx2txt",
    "antiword",
    "catdoc",
    "unrtf",
    "xlsx2csv",
    "strings",
    "file",
];

pub(super) fn render_document_parser_debug_json() -> String {
    let parser_provider_raw = env_with_fallback(
        "DASH_INGEST_DOCUMENT_PARSER_PROVIDER",
        "EME_INGEST_DOCUMENT_PARSER_PROVIDER",
    )
    .unwrap_or_else(|| DEFAULT_DOCUMENT_PARSER_PROVIDER.to_string());
    let parser_provider = normalize_document_parser_provider(&parser_provider_raw);
    let adapter_command = env_with_fallback(
        "DASH_INGEST_DOCUMENT_ADAPTER_CMD",
        "EME_INGEST_DOCUMENT_ADAPTER_CMD",
    )
    .map(|value| value.trim().to_string())
    .filter(|value| !value.is_empty());
    let adapter_command_preview = adapter_command
        .as_deref()
        .and_then(command_preview)
        .unwrap_or_default();
    let adapter_script_present = Path::new(DOCUMENT_ADAPTER_SCRIPT_PATH).exists();
    let embedding_provider_raw = env_with_fallback(
        "DASH_INGEST_EMBEDDING_PROVIDER",
        "EME_INGEST_EMBEDDING_PROVIDER",
    )
    .unwrap_or_else(|| DEFAULT_EMBEDDING_PROVIDER.to_string());
    let embedding_provider = normalize_embedding_provider(&embedding_provider_raw);
    let embedding_adapter_command = env_with_fallback(
        "DASH_INGEST_EMBEDDING_ADAPTER_CMD",
        "EME_INGEST_EMBEDDING_ADAPTER_CMD",
    )
    .map(|value| value.trim().to_string())
    .filter(|value| !value.is_empty());
    let embedding_adapter_command_preview = embedding_adapter_command
        .as_deref()
        .and_then(command_preview)
        .unwrap_or_default();
    let embedding_dimensions = env_with_fallback(
        "DASH_INGEST_EMBEDDING_DIMENSIONS",
        "EME_INGEST_EMBEDDING_DIMENSIONS",
    )
    .and_then(|value| value.trim().parse::<usize>().ok())
    .unwrap_or(DEFAULT_EMBEDDING_DIMENSIONS);

    let tooling = TOOL_NAMES
        .iter()
        .map(|tool| format!("\"{}\":{}", tool, command_exists(tool)))
        .collect::<Vec<_>>()
        .join(",");
    let builtin_mime_types = BUILTIN_UTF8_MIME_TYPES
        .iter()
        .map(|value| format!("\"{}\"", value))
        .collect::<Vec<_>>()
        .join(",");

    let mut warnings = Vec::new();
    if parser_provider == "adapter_command" && adapter_command.is_none() {
        warnings.push(
            "adapter_command parser requires DASH_INGEST_DOCUMENT_ADAPTER_CMD (or EME_*)"
                .to_string(),
        );
    }
    if parser_provider == "unknown" {
        warnings.push(format!(
            "unsupported document parser provider '{}'; expected builtin_utf8 or adapter_command",
            parser_provider_raw.trim()
        ));
    }
    if !adapter_script_present {
        warnings.push(format!(
            "recommended adapter script is missing at {}",
            DOCUMENT_ADAPTER_SCRIPT_PATH
        ));
    }
    if embedding_provider == "adapter_command" && embedding_adapter_command.is_none() {
        warnings.push(
            "adapter_command embedding provider requires DASH_INGEST_EMBEDDING_ADAPTER_CMD (or EME_*)"
                .to_string(),
        );
    }
    if embedding_provider == "unknown" {
        warnings.push(format!(
            "unsupported embedding provider '{}'; expected hash_vector or adapter_command",
            embedding_provider_raw.trim()
        ));
    }
    let warnings_json = warnings
        .iter()
        .map(|value| format!("\"{}\"", json_escape(value)))
        .collect::<Vec<_>>()
        .join(",");

    format!(
        "{{\"parser_provider\":\"{}\",\"parser_provider_raw\":\"{}\",\"adapter_command_configured\":{},\"adapter_command_preview\":\"{}\",\"embedding_provider\":\"{}\",\"embedding_provider_raw\":\"{}\",\"embedding_adapter_command_configured\":{},\"embedding_adapter_command_preview\":\"{}\",\"embedding_dimensions\":{},\"recommended_adapter_script\":\"{}\",\"recommended_adapter_script_present\":{},\"builtin_utf8_supported_mime_types\":[{}],\"tooling\":{{{}}},\"warnings\":[{}]}}",
        json_escape(parser_provider),
        json_escape(parser_provider_raw.trim()),
        adapter_command.is_some(),
        json_escape(&adapter_command_preview),
        json_escape(embedding_provider),
        json_escape(embedding_provider_raw.trim()),
        embedding_adapter_command.is_some(),
        json_escape(&embedding_adapter_command_preview),
        embedding_dimensions,
        json_escape(DOCUMENT_ADAPTER_SCRIPT_PATH),
        adapter_script_present,
        builtin_mime_types,
        tooling,
        warnings_json,
    )
}

fn normalize_document_parser_provider(raw: &str) -> &'static str {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() || normalized == "builtin_utf8" || normalized == "utf8" {
        return "builtin_utf8";
    }
    if normalized == "adapter_command" || normalized == "document_adapter" {
        return "adapter_command";
    }
    "unknown"
}

fn normalize_embedding_provider(raw: &str) -> &'static str {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || matches!(normalized.as_str(), "hash" | "hash_vector" | "builtin_hash")
    {
        return "hash_vector";
    }
    if normalized == "adapter_command" || normalized == "model_adapter" {
        return "adapter_command";
    }
    if matches!(normalized.as_str(), "off" | "none" | "disabled") {
        return "disabled";
    }
    "unknown"
}

fn command_preview(command: &str) -> Option<String> {
    command
        .split_whitespace()
        .find(|token| !token.is_empty())
        .map(|token| token.to_string())
}

fn command_exists(command: &str) -> bool {
    Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {} >/dev/null 2>&1", command))
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}
