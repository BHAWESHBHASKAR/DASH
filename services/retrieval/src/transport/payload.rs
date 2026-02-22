use super::*;

pub(super) fn build_retrieve_transport_request_from_query(
    query: &HashMap<String, String>,
) -> Result<RetrieveTransportRequest, String> {
    let tenant_id = query
        .get("tenant_id")
        .ok_or_else(|| "tenant_id is required".to_string())?
        .trim()
        .to_string();
    if tenant_id.is_empty() {
        return Err("tenant_id cannot be empty".to_string());
    }

    let request_query = query
        .get("query")
        .ok_or_else(|| "query is required".to_string())?
        .trim()
        .to_string();
    if request_query.is_empty() {
        return Err("query cannot be empty".to_string());
    }

    let query_embedding = query
        .get("query_embedding")
        .map(|value| parse_query_embedding_csv(value, "query_embedding"))
        .transpose()?;
    let entity_filters = query
        .get("entity_filters")
        .map(|value| parse_csv_string_list(value, "entity_filters"))
        .transpose()?
        .unwrap_or_default();
    let embedding_id_filters = query
        .get("embedding_id_filters")
        .map(|value| parse_csv_string_list(value, "embedding_id_filters"))
        .transpose()?
        .unwrap_or_default();

    let top_k = match query.get("top_k") {
        Some(value) => parse_positive_usize(value, "top_k")?,
        None => 5,
    };

    let stance_mode = match query.get("stance_mode").map(|s| s.as_str()) {
        Some("balanced") | None => StanceMode::Balanced,
        Some("support_only") => StanceMode::SupportOnly,
        Some(_) => return Err("stance_mode must be balanced or support_only".to_string()),
    };

    let return_graph = match query.get("return_graph").map(|s| s.as_str()) {
        Some("true") => true,
        Some("false") | None => false,
        Some(_) => return Err("return_graph must be true or false".to_string()),
    };
    let read_consistency =
        ReadConsistencyPolicy::from_raw(query.get("read_consistency").map(String::as_str))?;

    let from_unix = query
        .get("from_unix")
        .map(|value| parse_i64(value, "from_unix"))
        .transpose()?;
    let to_unix = query
        .get("to_unix")
        .map(|value| parse_i64(value, "to_unix"))
        .transpose()?;
    let time_range = if from_unix.is_some() || to_unix.is_some() {
        Some(TimeRange { from_unix, to_unix })
    } else {
        None
    };
    if let Some(TimeRange {
        from_unix: Some(from),
        to_unix: Some(to),
    }) = &time_range
        && from > to
    {
        return Err("time range is invalid: from_unix must be <= to_unix".to_string());
    }

    Ok(RetrieveTransportRequest {
        request: RetrieveApiRequest {
            tenant_id,
            query: request_query,
            query_embedding,
            entity_filters,
            embedding_id_filters,
            top_k,
            stance_mode,
            return_graph,
            time_range,
        },
        read_consistency,
    })
}

pub(super) fn build_retrieve_request_from_query(
    query: &HashMap<String, String>,
) -> Result<RetrieveApiRequest, String> {
    build_retrieve_transport_request_from_query(query).map(|value| value.request)
}

pub(super) fn build_retrieve_transport_request_from_json(
    body: &str,
) -> Result<RetrieveTransportRequest, String> {
    let value = parse_json(body)?;
    let object = match value {
        JsonValue::Object(map) => map,
        _ => return Err("request body must be a JSON object".to_string()),
    };

    let tenant_id = require_string(&object, "tenant_id")?;
    if tenant_id.trim().is_empty() {
        return Err("tenant_id cannot be empty".to_string());
    }

    let query = require_string(&object, "query")?;
    if query.trim().is_empty() {
        return Err("query cannot be empty".to_string());
    }

    let query_embedding =
        parse_optional_f32_array(object.get("query_embedding"), "query_embedding")?;
    let entity_filters =
        parse_optional_string_array(object.get("entity_filters"), "entity_filters")?
            .unwrap_or_default();
    let embedding_id_filters =
        parse_optional_string_array(object.get("embedding_id_filters"), "embedding_id_filters")?
            .unwrap_or_default();

    let top_k = match object.get("top_k") {
        Some(JsonValue::Number(raw)) => parse_positive_usize(raw, "top_k")?,
        Some(_) => return Err("top_k must be a positive integer".to_string()),
        None => 5,
    };

    let stance_mode = match object.get("stance_mode") {
        Some(JsonValue::String(mode)) => parse_stance_mode(mode)?,
        Some(_) => return Err("stance_mode must be a string".to_string()),
        None => StanceMode::Balanced,
    };

    let return_graph = match object.get("return_graph") {
        Some(JsonValue::Bool(flag)) => *flag,
        Some(_) => return Err("return_graph must be a boolean".to_string()),
        None => false,
    };
    let read_consistency = match object.get("read_consistency") {
        Some(JsonValue::String(value)) => ReadConsistencyPolicy::from_raw(Some(value))?,
        Some(JsonValue::Null) | None => ReadConsistencyPolicy::One,
        Some(_) => return Err("read_consistency must be a string".to_string()),
    };

    let time_range = match object.get("time_range") {
        Some(JsonValue::Object(range_obj)) => {
            let from_unix = match range_obj.get("from_unix") {
                Some(JsonValue::Number(raw)) => Some(parse_i64(raw, "time_range.from_unix")?),
                Some(JsonValue::Null) | None => None,
                Some(_) => {
                    return Err("time_range.from_unix must be an i64 timestamp".to_string());
                }
            };
            let to_unix = match range_obj.get("to_unix") {
                Some(JsonValue::Number(raw)) => Some(parse_i64(raw, "time_range.to_unix")?),
                Some(JsonValue::Null) | None => None,
                Some(_) => {
                    return Err("time_range.to_unix must be an i64 timestamp".to_string());
                }
            };

            if from_unix.is_some() || to_unix.is_some() {
                Some(TimeRange { from_unix, to_unix })
            } else {
                None
            }
        }
        Some(JsonValue::Null) | None => None,
        Some(_) => return Err("time_range must be an object or null".to_string()),
    };
    if let Some(TimeRange {
        from_unix: Some(from),
        to_unix: Some(to),
    }) = &time_range
        && from > to
    {
        return Err("time range is invalid: from_unix must be <= to_unix".to_string());
    }

    Ok(RetrieveTransportRequest {
        request: RetrieveApiRequest {
            tenant_id,
            query,
            query_embedding,
            entity_filters,
            embedding_id_filters,
            top_k,
            stance_mode,
            return_graph,
            time_range,
        },
        read_consistency,
    })
}

#[cfg(test)]
pub(super) fn build_retrieve_request_from_json(body: &str) -> Result<RetrieveApiRequest, String> {
    build_retrieve_transport_request_from_json(body).map(|value| value.request)
}

fn parse_stance_mode(raw: &str) -> Result<StanceMode, String> {
    match raw {
        "balanced" => Ok(StanceMode::Balanced),
        "support_only" => Ok(StanceMode::SupportOnly),
        _ => Err("stance_mode must be balanced or support_only".to_string()),
    }
}

fn require_string(map: &HashMap<String, JsonValue>, key: &str) -> Result<String, String> {
    match map.get(key) {
        Some(JsonValue::String(value)) => Ok(value.clone()),
        Some(_) => Err(format!("{key} must be a string")),
        None => Err(format!("{key} is required")),
    }
}

fn parse_positive_usize(raw: &str, field_name: &str) -> Result<usize, String> {
    if raw.contains('.') || raw.contains('e') || raw.contains('E') || raw.starts_with('-') {
        return Err(format!("{field_name} must be a positive integer"));
    }
    let parsed = raw
        .parse::<usize>()
        .map_err(|_| format!("{field_name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{field_name} must be > 0"));
    }
    Ok(parsed)
}

fn parse_i64(raw: &str, field_name: &str) -> Result<i64, String> {
    if raw.contains('.') || raw.contains('e') || raw.contains('E') {
        return Err(format!("{field_name} must be an i64 timestamp"));
    }
    raw.parse::<i64>()
        .map_err(|_| format!("{field_name} must be an i64 timestamp"))
}

fn parse_query_embedding_csv(raw: &str, field_name: &str) -> Result<Vec<f32>, String> {
    let values: Vec<&str> = raw
        .split(',')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .collect();
    if values.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }
    let mut out = Vec::with_capacity(values.len());
    for part in values {
        let parsed = part
            .parse::<f32>()
            .map_err(|_| format!("{field_name} must contain only numbers"))?;
        if !parsed.is_finite() {
            return Err(format!("{field_name} values must be finite"));
        }
        out.push(parsed);
    }
    Ok(out)
}

fn parse_csv_string_list(raw: &str, field_name: &str) -> Result<Vec<String>, String> {
    let out: Vec<String> = raw
        .split(',')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    if out.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }
    Ok(out)
}

fn parse_optional_f32_array(
    value: Option<&JsonValue>,
    field_name: &str,
) -> Result<Option<Vec<f32>>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let raw = match item {
                    JsonValue::Number(raw) => raw,
                    _ => return Err(format!("{field_name} must be an array of numbers")),
                };
                let parsed = raw
                    .parse::<f32>()
                    .map_err(|_| format!("{field_name} must be an array of numbers"))?;
                if !parsed.is_finite() {
                    return Err(format!("{field_name} values must be finite"));
                }
                out.push(parsed);
            }
            if out.is_empty() {
                return Err(format!("{field_name} must not be empty when provided"));
            }
            Ok(Some(out))
        }
        Some(_) => Err(format!("{field_name} must be an array or null")),
    }
}

fn parse_optional_string_array(
    value: Option<&JsonValue>,
    field_name: &str,
) -> Result<Option<Vec<String>>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let value = match item {
                    JsonValue::String(value) => value,
                    _ => return Err(format!("{field_name} must be an array of strings")),
                };
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    return Err(format!("{field_name} must not contain empty strings"));
                }
                out.push(trimmed.to_string());
            }
            Ok(Some(out))
        }
        Some(_) => Err(format!("{field_name} must be an array or null")),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum JsonValue {
    Object(HashMap<String, JsonValue>),
    Array(Vec<JsonValue>),
    String(String),
    Number(String),
    Bool(bool),
    Null,
}

pub(super) fn parse_json(input: &str) -> Result<JsonValue, String> {
    let mut parser = JsonParser::new(input);
    let value = parser.parse_value()?;
    parser.skip_whitespace();
    if !parser.is_eof() {
        return Err("unexpected trailing JSON content".to_string());
    }
    Ok(value)
}

struct JsonParser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            bytes: input.as_bytes(),
            pos: 0,
        }
    }

    fn parse_value(&mut self) -> Result<JsonValue, String> {
        self.skip_whitespace();
        match self.peek_byte() {
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'"') => self.parse_string().map(JsonValue::String),
            Some(b't') => {
                self.expect_literal("true")?;
                Ok(JsonValue::Bool(true))
            }
            Some(b'f') => {
                self.expect_literal("false")?;
                Ok(JsonValue::Bool(false))
            }
            Some(b'n') => {
                self.expect_literal("null")?;
                Ok(JsonValue::Null)
            }
            Some(b'-' | b'0'..=b'9') => self.parse_number().map(JsonValue::Number),
            Some(_) => Err("unsupported JSON token".to_string()),
            None => Err("empty JSON payload".to_string()),
        }
    }

    fn parse_object(&mut self) -> Result<JsonValue, String> {
        self.expect_byte(b'{')?;
        self.skip_whitespace();

        let mut map = HashMap::new();
        if self.peek_byte() == Some(b'}') {
            self.pos += 1;
            return Ok(JsonValue::Object(map));
        }

        loop {
            self.skip_whitespace();
            let key = self.parse_string()?;
            self.skip_whitespace();
            self.expect_byte(b':')?;
            let value = self.parse_value()?;
            map.insert(key, value);
            self.skip_whitespace();

            match self.peek_byte() {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b'}') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err("invalid JSON object".to_string()),
            }
        }

        Ok(JsonValue::Object(map))
    }

    fn parse_array(&mut self) -> Result<JsonValue, String> {
        self.expect_byte(b'[')?;
        self.skip_whitespace();

        let mut items = Vec::new();
        if self.peek_byte() == Some(b']') {
            self.pos += 1;
            return Ok(JsonValue::Array(items));
        }

        loop {
            let value = self.parse_value()?;
            items.push(value);
            self.skip_whitespace();
            match self.peek_byte() {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b']') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err("invalid JSON array".to_string()),
            }
        }

        Ok(JsonValue::Array(items))
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.expect_byte(b'"')?;
        let mut out = String::new();

        while let Some(byte) = self.next_byte() {
            match byte {
                b'"' => return Ok(out),
                b'\\' => {
                    let escaped = self
                        .next_byte()
                        .ok_or_else(|| "unterminated JSON escape".to_string())?;
                    match escaped {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{0008}'),
                        b'f' => out.push('\u{000C}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let code = self.parse_hex4()?;
                            let ch = char::from_u32(code)
                                .ok_or_else(|| "invalid unicode escape".to_string())?;
                            out.push(ch);
                        }
                        _ => return Err("invalid JSON escape sequence".to_string()),
                    }
                }
                b if b.is_ascii_control() => {
                    return Err("unescaped control character in JSON string".to_string());
                }
                b => out.push(b as char),
            }
        }

        Err("unterminated JSON string".to_string())
    }

    fn parse_number(&mut self) -> Result<String, String> {
        let start = self.pos;

        if self.peek_byte() == Some(b'-') {
            self.pos += 1;
        }

        match self.peek_byte() {
            Some(b'0') => {
                self.pos += 1;
            }
            Some(b'1'..=b'9') => {
                self.pos += 1;
                while matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                    self.pos += 1;
                }
            }
            _ => return Err("invalid JSON number".to_string()),
        }

        if self.peek_byte() == Some(b'.') {
            self.pos += 1;
            if !matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                return Err("invalid JSON number".to_string());
            }
            while matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }

        if matches!(self.peek_byte(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek_byte(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                return Err("invalid JSON number".to_string());
            }
            while matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }

        let raw = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| "invalid JSON number".to_string())?;
        Ok(raw.to_string())
    }

    fn parse_hex4(&mut self) -> Result<u32, String> {
        let mut value: u32 = 0;
        for _ in 0..4 {
            let byte = self
                .next_byte()
                .ok_or_else(|| "incomplete unicode escape".to_string())?;
            value = (value << 4)
                + match byte {
                    b'0'..=b'9' => (byte - b'0') as u32,
                    b'a'..=b'f' => (byte - b'a' + 10) as u32,
                    b'A'..=b'F' => (byte - b'A' + 10) as u32,
                    _ => return Err("invalid unicode escape".to_string()),
                };
        }
        Ok(value)
    }

    fn expect_literal(&mut self, literal: &str) -> Result<(), String> {
        let bytes = literal.as_bytes();
        if self.bytes.get(self.pos..self.pos + bytes.len()) == Some(bytes) {
            self.pos += bytes.len();
            Ok(())
        } else {
            Err("invalid JSON literal".to_string())
        }
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), String> {
        match self.next_byte() {
            Some(byte) if byte == expected => Ok(()),
            _ => Err("invalid JSON syntax".to_string()),
        }
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek_byte(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.pos += 1;
        }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek_byte(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn next_byte(&mut self) -> Option<u8> {
        let out = self.peek_byte()?;
        self.pos += 1;
        Some(out)
    }
}

pub(super) fn render_retrieve_response_json(resp: &crate::api::RetrieveApiResponse) -> String {
    let mut out = String::new();
    out.push_str("{\"results\":[");
    for (idx, node) in resp.results.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        render_evidence_node_json(&mut out, node);
    }
    out.push_str("],\"graph\":");

    if let Some(graph) = &resp.graph {
        out.push_str("{\"nodes\":[");
        for (idx, node) in graph.nodes.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            render_evidence_node_json(&mut out, node);
        }
        out.push_str("],\"edges\":[");
        for (idx, edge) in graph.edges.iter().enumerate() {
            if idx > 0 {
                out.push(',');
            }
            out.push('{');
            out.push_str("\"from_claim_id\":\"");
            out.push_str(&json_escape(&edge.from_claim_id));
            out.push_str("\",\"to_claim_id\":\"");
            out.push_str(&json_escape(&edge.to_claim_id));
            out.push_str("\",\"relation\":\"");
            out.push_str(&json_escape(&edge.relation));
            out.push_str("\",\"strength\":");
            out.push_str(&format!("{:.6}", edge.strength));
            out.push('}');
        }
        out.push_str("]}");
    } else {
        out.push_str("null");
    }

    out.push('}');
    out
}

fn render_evidence_node_json(out: &mut String, node: &crate::api::EvidenceNode) {
    out.push('{');
    out.push_str("\"claim_id\":\"");
    out.push_str(&json_escape(&node.claim_id));
    out.push_str("\",\"canonical_text\":\"");
    out.push_str(&json_escape(&node.canonical_text));
    out.push_str("\",\"score\":");
    out.push_str(&format!("{:.6}", node.score));
    out.push_str(",\"claim_confidence\":");
    render_optional_f32(out, node.claim_confidence);
    out.push_str(",\"confidence_band\":");
    render_optional_string(out, node.confidence_band.as_deref());
    out.push_str(",\"dominant_stance\":");
    render_optional_string(out, node.dominant_stance.as_deref());
    out.push_str(",\"contradiction_risk\":");
    render_optional_f32(out, node.contradiction_risk);
    out.push_str(",\"graph_score\":");
    render_optional_f32(out, node.graph_score);
    out.push_str(",\"support_path_count\":");
    render_optional_usize(out, node.support_path_count);
    out.push_str(",\"contradiction_chain_depth\":");
    render_optional_usize(out, node.contradiction_chain_depth);
    out.push_str(",\"supports\":");
    out.push_str(&node.supports.to_string());
    out.push_str(",\"contradicts\":");
    out.push_str(&node.contradicts.to_string());
    out.push_str(",\"citations\":");
    out.push_str(&render_citations_json(&node.citations));
    out.push_str(",\"event_time_unix\":");
    render_optional_i64(out, node.event_time_unix);
    out.push_str(",\"temporal_match_mode\":");
    render_optional_string(out, node.temporal_match_mode.as_deref());
    out.push_str(",\"temporal_in_range\":");
    render_optional_bool(out, node.temporal_in_range);
    out.push_str(",\"claim_type\":");
    render_optional_string(out, node.claim_type.as_deref());
    out.push_str(",\"valid_from\":");
    render_optional_i64(out, node.valid_from);
    out.push_str(",\"valid_to\":");
    render_optional_i64(out, node.valid_to);
    out.push_str(",\"created_at\":");
    render_optional_i64(out, node.created_at);
    out.push_str(",\"updated_at\":");
    render_optional_i64(out, node.updated_at);
    out.push('}');
}

fn render_optional_i64(out: &mut String, value: Option<i64>) {
    if let Some(value) = value {
        out.push_str(&value.to_string());
    } else {
        out.push_str("null");
    }
}

fn render_optional_f32(out: &mut String, value: Option<f32>) {
    if let Some(value) = value {
        out.push_str(&format!("{:.6}", value));
    } else {
        out.push_str("null");
    }
}

fn render_optional_usize(out: &mut String, value: Option<usize>) {
    if let Some(value) = value {
        out.push_str(&value.to_string());
    } else {
        out.push_str("null");
    }
}

fn render_optional_bool(out: &mut String, value: Option<bool>) {
    if let Some(value) = value {
        out.push_str(if value { "true" } else { "false" });
    } else {
        out.push_str("null");
    }
}

fn render_optional_string(out: &mut String, value: Option<&str>) {
    if let Some(value) = value {
        out.push('"');
        out.push_str(&json_escape(value));
        out.push('"');
    } else {
        out.push_str("null");
    }
}

fn render_citations_json(citations: &[CitationNode]) -> String {
    let mut out = String::new();
    out.push('[');
    for (idx, citation) in citations.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push('{');
        out.push_str("\"evidence_id\":\"");
        out.push_str(&json_escape(&citation.evidence_id));
        out.push_str("\",\"source_id\":\"");
        out.push_str(&json_escape(&citation.source_id));
        out.push_str("\",\"stance\":\"");
        out.push_str(&json_escape(&citation.stance));
        out.push_str("\",\"source_quality\":");
        out.push_str(&format!("{:.6}", citation.source_quality));
        out.push_str(",\"chunk_id\":");
        if let Some(chunk_id) = &citation.chunk_id {
            out.push('"');
            out.push_str(&json_escape(chunk_id));
            out.push('"');
        } else {
            out.push_str("null");
        }
        out.push_str(",\"span_start\":");
        if let Some(span_start) = citation.span_start {
            out.push_str(&span_start.to_string());
        } else {
            out.push_str("null");
        }
        out.push_str(",\"span_end\":");
        if let Some(span_end) = citation.span_end {
            out.push_str(&span_end.to_string());
        } else {
            out.push_str("null");
        }
        out.push_str(",\"doc_id\":");
        render_optional_string(&mut out, citation.doc_id.as_deref());
        out.push_str(",\"extraction_model\":");
        render_optional_string(&mut out, citation.extraction_model.as_deref());
        out.push_str(",\"ingested_at\":");
        render_optional_i64(&mut out, citation.ingested_at);
        out.push('}');
    }
    out.push(']');
    out
}

pub(super) fn json_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out
}
