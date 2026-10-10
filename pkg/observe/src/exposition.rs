//! Prometheus text exposition (format 0.0.4): a writer and a strict
//! validator.
//!
//! The validator is stricter than Prometheus itself so that `/metrics` stays
//! clean as it grows: every sample must belong to a family declared with
//! `# TYPE`, a family is declared once and its samples are contiguous, no
//! series appears twice, counters are non-negative, and every histogram
//! series has cumulative buckets ending in `+Inf` that equal its `_count`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;

use crate::histogram::HistogramSnapshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
    Summary,
    Untyped,
}

impl MetricKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "histogram",
            MetricKind::Summary => "summary",
            MetricKind::Untyped => "untyped",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "counter" => MetricKind::Counter,
            "gauge" => MetricKind::Gauge,
            "histogram" => MetricKind::Histogram,
            "summary" => MetricKind::Summary,
            "untyped" => MetricKind::Untyped,
            _ => return None,
        })
    }
}

/// Escape a label value (`\`, `"` and newline).
pub fn escape_label_value(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
    out
}

/// Escape `# HELP` text (`\` and newline).
fn escape_help(raw: &str) -> String {
    raw.replace('\\', "\\\\").replace('\n', "\\n")
}

/// Render a sample value: integral values without a fractional part,
/// infinities as `+Inf` / `-Inf`.
pub fn format_value(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value > 0.0 { "+Inf" } else { "-Inf" }.to_string()
    } else if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

/// Builds a Prometheus text body. Each family header is written once, even
/// when several call sites add series to it.
#[derive(Debug, Default)]
pub struct MetricsWriter {
    out: String,
    declared: HashSet<String>,
}

impl MetricsWriter {
    pub fn new() -> Self {
        Self::default()
    }

    /// `# HELP` and `# TYPE` for `name`, unless already written.
    pub fn header(&mut self, name: &str, help: &str, kind: MetricKind) {
        if self.declared.insert(name.to_string()) {
            let _ = writeln!(self.out, "# HELP {name} {}", escape_help(help));
            let _ = writeln!(self.out, "# TYPE {name} {}", kind.as_str());
        }
    }

    /// One sample line. Call [`MetricsWriter::header`] for its family first.
    pub fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: f64) {
        self.out.push_str(name);
        write_labels(&mut self.out, labels, None);
        self.out.push(' ');
        self.out.push_str(&format_value(value));
        self.out.push('\n');
    }

    pub fn counter(&mut self, name: &str, help: &str, value: f64) {
        self.header(name, help, MetricKind::Counter);
        self.sample(name, &[], value);
    }

    pub fn gauge(&mut self, name: &str, help: &str, value: f64) {
        self.header(name, help, MetricKind::Gauge);
        self.sample(name, &[], value);
    }

    /// Header plus one histogram series.
    pub fn histogram(
        &mut self,
        name: &str,
        help: &str,
        labels: &[(&str, &str)],
        snapshot: &HistogramSnapshot,
    ) {
        self.header(name, help, MetricKind::Histogram);
        self.histogram_series(name, labels, snapshot);
    }

    /// One histogram series (`_bucket` lines, `_sum`, `_count`).
    pub fn histogram_series(
        &mut self,
        name: &str,
        labels: &[(&str, &str)],
        snapshot: &HistogramSnapshot,
    ) {
        let mut cumulative = 0u64;
        for (idx, bound) in snapshot.bounds.iter().enumerate() {
            cumulative += snapshot.buckets[idx];
            let le = format_value(*bound);
            let _ = write!(self.out, "{name}_bucket");
            write_labels(&mut self.out, labels, Some(&le));
            let _ = writeln!(self.out, " {cumulative}");
        }
        cumulative += snapshot.buckets[snapshot.bounds.len()];
        let _ = write!(self.out, "{name}_bucket");
        write_labels(&mut self.out, labels, Some("+Inf"));
        let _ = writeln!(self.out, " {cumulative}");
        let _ = write!(self.out, "{name}_sum");
        write_labels(&mut self.out, labels, None);
        let _ = writeln!(self.out, " {}", format_value(snapshot.sum));
        let _ = write!(self.out, "{name}_count");
        write_labels(&mut self.out, labels, None);
        let _ = writeln!(self.out, " {cumulative}");
    }

    /// Append pre-rendered exposition text (for example from a component
    /// that still formats its own lines).
    pub fn raw(&mut self, text: &str) {
        self.out.push_str(text);
        if !text.is_empty() && !text.ends_with('\n') {
            self.out.push('\n');
        }
    }

    pub fn finish(self) -> String {
        self.out
    }
}

fn write_labels(out: &mut String, labels: &[(&str, &str)], le: Option<&str>) {
    if labels.is_empty() && le.is_none() {
        return;
    }
    out.push('{');
    let mut first = true;
    for (name, value) in labels {
        if !first {
            out.push(',');
        }
        first = false;
        let _ = write!(out, "{name}=\"{}\"", escape_label_value(value));
    }
    if let Some(le) = le {
        if !first {
            out.push(',');
        }
        let _ = write!(out, "le=\"{le}\"");
    }
    out.push('}');
}

/// What [`validate`] found in a valid body.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ValidationReport {
    /// Declared family name -> type.
    pub families: BTreeMap<String, MetricKind>,
    pub samples: usize,
    /// Every series as `name{label="value",...}` (labels sorted), with its value.
    pub series: BTreeMap<String, f64>,
}

impl ValidationReport {
    pub fn has_family(&self, name: &str) -> bool {
        self.families.contains_key(name)
    }

    pub fn kind(&self, name: &str) -> Option<MetricKind> {
        self.families.get(name).copied()
    }

    /// Value of the series with exactly these labels.
    pub fn value(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let mut sorted: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        sorted.sort();
        self.series.get(&series_key(name, &sorted)).copied()
    }

    /// Sum of every series of `name` whose labels include all of `matching`.
    pub fn sum_where(&self, name: &str, matching: &[(&str, &str)]) -> f64 {
        self.series
            .iter()
            .filter(|(key, _)| {
                let (series_name, labels) = key.split_once('{').unwrap_or((key.as_str(), ""));
                series_name == name
                    && matching.iter().all(|(k, v)| {
                        labels.contains(&format!("{k}=\"{}\"", escape_label_value(v)))
                    })
            })
            .map(|(_, v)| *v)
            .sum()
    }
}

fn series_key(name: &str, labels: &[(String, String)]) -> String {
    if labels.is_empty() {
        return name.to_string();
    }
    let rendered: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", escape_label_value(v)))
        .collect();
    format!("{name}{{{}}}", rendered.join(","))
}

fn valid_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_' || c == ':')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

fn valid_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn parse_value(raw: &str) -> Option<f64> {
    match raw {
        "+Inf" | "Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        _ => {
            // Reject forms Rust accepts but Prometheus does not ("inf", "nan").
            if raw
                .chars()
                .any(|c| c.is_ascii_alphabetic() && c != 'e' && c != 'E')
            {
                return None;
            }
            raw.parse::<f64>().ok()
        }
    }
}

struct ParsedSample {
    name: String,
    labels: Vec<(String, String)>,
    value: f64,
}

fn parse_sample(line: &str) -> Result<ParsedSample, String> {
    let name_end = line
        .find(|c: char| c == '{' || c == ' ' || c == '\t')
        .ok_or("sample has no value")?;
    let name = &line[..name_end];
    if !valid_metric_name(name) {
        return Err(format!("invalid metric name '{name}'"));
    }
    let mut rest = &line[name_end..];
    let mut labels: Vec<(String, String)> = Vec::new();
    if let Some(after) = rest.strip_prefix('{') {
        let mut chars = after.char_indices().peekable();
        let consumed;
        loop {
            // skip whitespace
            while matches!(chars.peek(), Some((_, c)) if *c == ' ') {
                chars.next();
            }
            match chars.peek() {
                Some((idx, '}')) => {
                    consumed = idx + 1;
                    break;
                }
                None => return Err("unterminated label set".into()),
                _ => {}
            }
            let mut label_name = String::new();
            while let Some((_, c)) = chars.peek() {
                if *c == '=' {
                    break;
                }
                label_name.push(*c);
                chars.next();
            }
            if !valid_label_name(&label_name) {
                return Err(format!("invalid label name '{label_name}'"));
            }
            if chars.next().map(|(_, c)| c) != Some('=') {
                return Err("expected '=' after label name".into());
            }
            if chars.next().map(|(_, c)| c) != Some('"') {
                return Err("label value must be quoted".into());
            }
            let mut value = String::new();
            loop {
                match chars.next() {
                    Some((_, '\\')) => match chars.next() {
                        Some((_, '\\')) => value.push('\\'),
                        Some((_, '"')) => value.push('"'),
                        Some((_, 'n')) => value.push('\n'),
                        _ => return Err("invalid escape in label value".into()),
                    },
                    Some((_, '"')) => break,
                    Some((_, '\n')) | None => return Err("unterminated label value".into()),
                    Some((_, c)) => value.push(c),
                }
            }
            if labels.iter().any(|(k, _)| *k == label_name) {
                return Err(format!("duplicate label '{label_name}'"));
            }
            labels.push((label_name, value));
            match chars.next() {
                Some((_, ',')) => continue,
                Some((idx, '}')) => {
                    consumed = idx + 1;
                    break;
                }
                _ => return Err("expected ',' or '}' after label value".into()),
            }
        }
        rest = &after[consumed..];
    }
    let mut fields = rest.split_whitespace();
    let raw_value = fields.next().ok_or("sample has no value")?;
    let value = parse_value(raw_value).ok_or_else(|| format!("invalid value '{raw_value}'"))?;
    if let Some(ts) = fields.next()
        && ts.parse::<i64>().is_err()
    {
        return Err(format!("invalid timestamp '{ts}'"));
    }
    if fields.next().is_some() {
        return Err("trailing characters after sample".into());
    }
    if !rest.starts_with([' ', '\t']) {
        return Err("missing whitespace before value".into());
    }
    labels.sort();
    Ok(ParsedSample {
        name: name.to_string(),
        labels,
        value,
    })
}

#[derive(Default)]
struct HistogramSeries {
    buckets: Vec<(f64, f64)>,
    count: Option<f64>,
    sum: Option<f64>,
}

/// Strictly validate a Prometheus text exposition body.
pub fn validate(text: &str) -> Result<ValidationReport, String> {
    let mut report = ValidationReport::default();
    let mut help_seen: HashSet<String> = HashSet::new();
    let mut families_with_samples: HashSet<String> = HashSet::new();
    let mut closed: HashSet<String> = HashSet::new();
    let mut current: Option<String> = None;
    let mut histograms: HashMap<(String, String), HistogramSeries> = HashMap::new();

    for (idx, line) in text.lines().enumerate() {
        let lineno = idx + 1;
        let err = |msg: String| format!("line {lineno}: {msg}: {line:?}");
        if line.trim().is_empty() {
            continue;
        }
        if let Some(comment) = line.strip_prefix('#') {
            let comment = comment.trim_start();
            if let Some(rest) = comment.strip_prefix("HELP ") {
                let name = rest.split_whitespace().next().unwrap_or("");
                if !valid_metric_name(name) {
                    return Err(err("invalid metric name in HELP".into()));
                }
                if !help_seen.insert(name.to_string()) {
                    return Err(err(format!("second HELP line for {name}")));
                }
            } else if let Some(rest) = comment.strip_prefix("TYPE ") {
                let mut parts = rest.split_whitespace();
                let name = parts.next().unwrap_or("");
                let kind = parts.next().unwrap_or("");
                if !valid_metric_name(name) {
                    return Err(err("invalid metric name in TYPE".into()));
                }
                let Some(kind) = MetricKind::parse(kind) else {
                    return Err(err(format!("unknown metric type '{kind}'")));
                };
                if parts.next().is_some() {
                    return Err(err("trailing text after TYPE".into()));
                }
                if report.families.insert(name.to_string(), kind).is_some() {
                    return Err(err(format!("second TYPE line for {name}")));
                }
                if families_with_samples.contains(name) {
                    return Err(err(format!("TYPE for {name} after its samples")));
                }
            }
            continue;
        }

        let sample = parse_sample(line).map_err(err)?;
        let (family, suffix) = resolve_family(&report.families, &sample.name)
            .ok_or_else(|| err(format!("sample '{}' has no # TYPE", sample.name)))?;
        let kind = report.families[&family];

        if current.as_deref() != Some(family.as_str()) {
            if closed.contains(&family) {
                return Err(err(format!("samples of {family} are not contiguous")));
            }
            if let Some(previous) = current.take() {
                closed.insert(previous);
            }
            current = Some(family.clone());
        }
        families_with_samples.insert(family.clone());

        let key = series_key(&sample.name, &sample.labels);
        if report.series.insert(key.clone(), sample.value).is_some() {
            return Err(err(format!("duplicate series {key}")));
        }
        report.samples += 1;

        if kind == MetricKind::Counter && !(sample.value >= 0.0) {
            return Err(err("counter value must be a non-negative number".into()));
        }
        if kind == MetricKind::Histogram {
            let without_le: Vec<(String, String)> = sample
                .labels
                .iter()
                .filter(|(k, _)| k != "le")
                .cloned()
                .collect();
            let group = histograms
                .entry((family.clone(), series_key("", &without_le)))
                .or_default();
            match suffix {
                "_bucket" => {
                    let le = sample
                        .labels
                        .iter()
                        .find(|(k, _)| k == "le")
                        .map(|(_, v)| v.as_str())
                        .ok_or_else(|| err("histogram bucket without le label".into()))?;
                    let bound = parse_value(le)
                        .filter(|b| !b.is_nan())
                        .ok_or_else(|| err(format!("invalid le '{le}'")))?;
                    group.buckets.push((bound, sample.value));
                }
                "_count" => group.count = Some(sample.value),
                "_sum" => group.sum = Some(sample.value),
                _ => return Err(err("histogram sample without _bucket/_sum/_count".into())),
            }
        }
    }

    for ((family, labels), mut series) in histograms {
        let what = format!("histogram {family}{labels}");
        series.buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
        if series.buckets.windows(2).any(|w| w[0].0 == w[1].0) {
            return Err(format!("{what}: duplicate le"));
        }
        if series.buckets.windows(2).any(|w| w[1].1 < w[0].1) {
            return Err(format!("{what}: buckets are not cumulative"));
        }
        let Some(&(last_bound, last_value)) = series.buckets.last() else {
            return Err(format!("{what}: no buckets"));
        };
        if last_bound != f64::INFINITY {
            return Err(format!("{what}: missing le=\"+Inf\" bucket"));
        }
        match series.count {
            Some(count) if count == last_value => {}
            Some(count) => {
                return Err(format!(
                    "{what}: +Inf bucket {last_value} differs from _count {count}"
                ));
            }
            None => return Err(format!("{what}: missing _count")),
        }
        if series.sum.is_none() {
            return Err(format!("{what}: missing _sum"));
        }
    }
    Ok(report)
}

fn resolve_family(
    families: &BTreeMap<String, MetricKind>,
    sample_name: &str,
) -> Option<(String, &'static str)> {
    for suffix in ["_bucket", "_count", "_sum"] {
        if let Some(base) = sample_name.strip_suffix(suffix)
            && matches!(
                families.get(base),
                Some(MetricKind::Histogram | MetricKind::Summary)
            )
        {
            return Some((base.to_string(), suffix));
        }
    }
    families
        .contains_key(sample_name)
        .then(|| (sample_name.to_string(), ""))
}

/// Names of every family declared in `text` (helper for tests that compare
/// families across components).
pub fn family_names(text: &str) -> BTreeSet<String> {
    text.lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|rest| rest.split_whitespace().next())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::histogram::Histogram;

    #[test]
    fn writer_output_validates_and_round_trips_values() {
        let mut w = MetricsWriter::new();
        w.counter("dash_test_total", "A counter.", 3.0);
        w.gauge("dash_test_ratio", "A gauge.", 0.25);
        let h = Histogram::new(&[0.1, 1.0]);
        h.observe(0.05);
        h.observe(0.5);
        h.observe(3.0);
        w.histogram(
            "dash_test_seconds",
            "A histogram.",
            &[("route", "a\"b\\c\nd")],
            &h.snapshot(),
        );
        w.histogram_series("dash_test_seconds", &[("route", "other")], &h.snapshot());
        let text = w.finish();
        let report = validate(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
        assert_eq!(
            report.kind("dash_test_seconds"),
            Some(MetricKind::Histogram)
        );
        assert_eq!(report.value("dash_test_total", &[]), Some(3.0));
        assert_eq!(
            report.value(
                "dash_test_seconds_bucket",
                &[("route", "other"), ("le", "1")]
            ),
            Some(2.0)
        );
        assert_eq!(
            report.value("dash_test_seconds_count", &[("route", "a\"b\\c\nd")]),
            Some(3.0)
        );
        assert_eq!(report.sum_where("dash_test_seconds_count", &[]), 6.0);
        // A header is written once even when requested again.
        let mut w = MetricsWriter::new();
        w.header("x", "h", MetricKind::Gauge);
        w.header("x", "h", MetricKind::Gauge);
        assert_eq!(w.finish().matches("# TYPE x").count(), 1);
    }

    #[test]
    fn validator_rejects_malformed_bodies() {
        let cases: &[(&str, &str)] = &[
            ("x 1\n", "has no # TYPE"),
            ("# TYPE x gauge\n# TYPE x gauge\nx 1\n", "second TYPE"),
            ("# TYPE x gauge\nx 1\nx 2\n", "duplicate series"),
            (
                "# TYPE x gauge\nx 1\n# TYPE y gauge\ny 1\nx{a=\"1\"} 2\n",
                "not contiguous",
            ),
            ("# TYPE x counter\nx -1\n", "non-negative"),
            ("# TYPE x gauge\nx abc\n", "invalid value"),
            ("# TYPE x gauge\nx{a=b} 1\n", "quoted"),
            ("# TYPE x gauge\nx{a=\"b\",a=\"c\"} 1\n", "duplicate label"),
            ("# TYPE 1x gauge\n", "invalid metric name"),
            ("# TYPE x widget\n", "unknown metric type"),
            (
                "# TYPE h histogram\nh_bucket{le=\"1\"} 2\nh_bucket{le=\"+Inf\"} 1\nh_sum 1\nh_count 1\n",
                "not cumulative",
            ),
            (
                "# TYPE h histogram\nh_bucket{le=\"1\"} 1\nh_sum 1\nh_count 1\n",
                "+Inf",
            ),
            (
                "# TYPE h histogram\nh_bucket{le=\"+Inf\"} 2\nh_sum 1\nh_count 1\n",
                "differs from _count",
            ),
            (
                "# TYPE h histogram\nh_bucket{le=\"+Inf\"} 1\nh_count 1\n",
                "missing _sum",
            ),
            ("# TYPE h histogram\nh_bucket 1\n", "without le"),
            ("# TYPE x gauge\nx 1 notatimestamp\n", "timestamp"),
            ("# TYPE x gauge\nx{a=\"1\" 1\n", "expected ','"),
        ];
        for (body, expected) in cases {
            let err = validate(body).expect_err(body);
            assert!(err.contains(expected), "{body:?}: {err}");
        }
    }

    #[test]
    fn validator_accepts_the_spec_examples() {
        let body = "# HELP http_requests_total The total number of HTTP requests.\n\
# TYPE http_requests_total counter\n\
http_requests_total{method=\"post\",code=\"200\"} 1027 1395066363000\n\
http_requests_total{method=\"post\",code=\"400\"}    3 1395066363000\n\
\n\
# A normal comment.\n\
# TYPE metric_without_timestamp_and_labels gauge\n\
metric_without_timestamp_and_labels 12.47\n\
# TYPE something_weird gauge\n\
something_weird{problem=\"division by zero\"} +Inf -3982045\n\
# TYPE nan_gauge gauge\n\
nan_gauge NaN\n\
# TYPE exp gauge\n\
exp 1.5e-3\n";
        let report = validate(body).unwrap();
        assert_eq!(report.samples, 6);
        assert_eq!(family_names(body).len(), 5);
    }

    #[test]
    fn values_are_rendered_in_prometheus_form() {
        assert_eq!(format_value(3.0), "3");
        assert_eq!(format_value(0.0005), "0.0005");
        assert_eq!(format_value(f64::INFINITY), "+Inf");
        assert_eq!(format_value(f64::NEG_INFINITY), "-Inf");
        assert_eq!(format_value(f64::NAN), "NaN");
        assert_eq!(format_value(1e20), "100000000000000000000");
    }
}
