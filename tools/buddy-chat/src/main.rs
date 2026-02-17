use std::{
    collections::VecDeque,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

const DEFAULT_ROOT: &str = ".buddy";
const CHAT_DATABASE_FILE: &str = "chat-database.md";
const PLAN_FILE: &str = "plan.md";

fn main() {
    match run(std::env::args().collect()) {
        Ok(output) => {
            if !output.is_empty() {
                println!("{output}");
            }
        }
        Err(err) => {
            eprintln!("buddy-chat error: {err}");
            std::process::exit(2);
        }
    }
}

fn run(args: Vec<String>) -> Result<String, String> {
    let mut args: VecDeque<String> = args.into_iter().skip(1).collect();
    if args.is_empty() {
        return Ok(usage());
    }

    if matches!(
        args.front().map(String::as_str),
        Some("help" | "--help" | "-h")
    ) {
        return Ok(usage());
    }

    let root = parse_root(&mut args)?;
    let root_path = PathBuf::from(root);

    let cmd = pop_required(&mut args, "command")?;
    match cmd.as_str() {
        "init" => cmd_init(&root_path, &mut args),
        "thread" => cmd_thread(&root_path, &mut args),
        "send" => cmd_send(&root_path, &mut args),
        "tail" => cmd_tail(&root_path, &mut args),
        "task" => cmd_task(&root_path, &mut args),
        _ => Err(format!("unknown command '{cmd}'\n\n{}", usage())),
    }
}

fn usage() -> String {
    [
        "Usage: buddy-chat [--root PATH] <command>",
        "",
        "Commands:",
        "  init [--agents codex,buddy]",
        "  thread create <thread_id> [--title title]",
        "  thread list",
        "  send --thread <thread_id> --from <agent> --text <message> [--kind status|task|question|answer|decision]",
        "  tail --thread <thread_id> [--lines N]",
        "  task create --id <task_id> --title <title> --owner <agent> --why <why> --where <scope> --when <milestone_or_deadline> [--desc description]",
        "  task list [--status open|done|all]",
        "  task done --id <task_id> --by <agent> [--note note]",
    ]
    .join("\n")
}

fn parse_root(args: &mut VecDeque<String>) -> Result<String, String> {
    if matches!(args.front().map(String::as_str), Some("--root")) {
        args.pop_front();
        return pop_required(args, "--root value");
    }
    Ok(DEFAULT_ROOT.to_string())
}

fn cmd_init(root: &Path, args: &mut VecDeque<String>) -> Result<String, String> {
    ensure_layout(root)?;
    let mut agents = Vec::new();

    while let Some(flag) = args.pop_front() {
        match flag.as_str() {
            "--agents" => {
                let value = pop_required(args, "--agents value")?;
                agents = parse_agents_csv(&value);
                if agents.is_empty() {
                    return Err("--agents must include at least one agent".to_string());
                }
            }
            _ => return Err(format!("unknown init option '{flag}'")),
        }
    }

    if !agents.is_empty() {
        let agents_path = root.join("agents.txt");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(agents_path)
            .map_err(|e| e.to_string())?;
        for agent in &agents {
            writeln!(file, "{agent}").map_err(|e| e.to_string())?;
        }
    }

    let mut readme = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(root.join("README.md"))
        .map_err(|e| e.to_string())?;
    writeln!(readme, "# Buddy Chat Workspace").map_err(|e| e.to_string())?;
    writeln!(readme).map_err(|e| e.to_string())?;
    writeln!(readme, "- threads: {}/threads", root.display()).map_err(|e| e.to_string())?;
    writeln!(readme, "- tasks/open: {}/tasks/open", root.display()).map_err(|e| e.to_string())?;
    writeln!(readme, "- tasks/done: {}/tasks/done", root.display()).map_err(|e| e.to_string())?;
    writeln!(
        readme,
        "- chat_database: {}/{}",
        root.display(),
        CHAT_DATABASE_FILE
    )
    .map_err(|e| e.to_string())?;
    writeln!(readme, "- plan: {}/{}", root.display(), PLAN_FILE).map_err(|e| e.to_string())?;

    ensure_chat_database(root)?;
    ensure_plan_template(root)?;

    let agent_note = if agents.is_empty() {
        "none registered".to_string()
    } else {
        agents.join(",")
    };

    Ok(format!(
        "initialized buddy workspace at {} (agents: {agent_note})",
        root.display()
    ))
}

fn cmd_thread(root: &Path, args: &mut VecDeque<String>) -> Result<String, String> {
    ensure_layout(root)?;
    let sub = pop_required(args, "thread subcommand")?;
    match sub.as_str() {
        "create" => {
            let thread_id = pop_required(args, "thread_id")?;
            validate_id(&thread_id, "thread_id")?;

            let mut title = String::new();
            while let Some(flag) = args.pop_front() {
                match flag.as_str() {
                    "--title" => title = pop_required(args, "--title value")?,
                    _ => return Err(format!("unknown thread create option '{flag}'")),
                }
            }

            let log_path = thread_log_path(root, &thread_id);
            if log_path.exists() {
                return Err(format!("thread '{}' already exists", thread_id));
            }

            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&log_path)
                .map_err(|e| e.to_string())?;

            let mut meta = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(thread_meta_path(root, &thread_id))
                .map_err(|e| e.to_string())?;
            writeln!(meta, "id={thread_id}").map_err(|e| e.to_string())?;
            writeln!(meta, "created_epoch={}", now_epoch_secs()).map_err(|e| e.to_string())?;
            if !title.trim().is_empty() {
                writeln!(meta, "title={}", escape_field(title.trim()))
                    .map_err(|e| e.to_string())?;
            }

            Ok(format!("created thread '{}'", thread_id))
        }
        "list" => {
            let threads_dir = root.join("threads");
            let mut ids = Vec::new();
            for entry in fs::read_dir(threads_dir).map_err(|e| e.to_string())? {
                let entry = entry.map_err(|e| e.to_string())?;
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("log")
                    && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                {
                    ids.push(stem.to_string());
                }
            }
            ids.sort();
            if ids.is_empty() {
                Ok("no threads".to_string())
            } else {
                Ok(ids.join("\n"))
            }
        }
        _ => Err(format!("unknown thread subcommand '{sub}'")),
    }
}

fn cmd_send(root: &Path, args: &mut VecDeque<String>) -> Result<String, String> {
    ensure_layout(root)?;

    let mut thread_id = String::new();
    let mut from = String::new();
    let mut text = String::new();
    let mut kind = "status".to_string();

    while let Some(flag) = args.pop_front() {
        match flag.as_str() {
            "--thread" => thread_id = pop_required(args, "--thread value")?,
            "--from" => from = pop_required(args, "--from value")?,
            "--text" => text = pop_required(args, "--text value")?,
            "--kind" => kind = pop_required(args, "--kind value")?,
            _ => return Err(format!("unknown send option '{flag}'")),
        }
    }

    if thread_id.is_empty() || from.is_empty() || text.is_empty() {
        return Err("send requires --thread, --from, and --text".to_string());
    }
    validate_id(&thread_id, "thread_id")?;
    validate_kind(&kind)?;
    validate_registered_agent(root, &from)?;

    let path = thread_log_path(root, &thread_id);
    if !path.exists() {
        return Err(format!("thread '{}' does not exist", thread_id));
    }

    let mut file = OpenOptions::new()
        .create(false)
        .append(true)
        .open(&path)
        .map_err(|e| e.to_string())?;

    let epoch = now_epoch_secs();
    writeln!(
        file,
        "{}\t{}\t{}\t{}",
        epoch,
        escape_field(&from),
        escape_field(&kind),
        escape_field(&text)
    )
    .map_err(|e| e.to_string())?;

    append_chat_database_message(root, epoch, &thread_id, &from, &kind, &text)?;

    Ok(format!("posted message to thread '{}'", thread_id))
}

fn cmd_tail(root: &Path, args: &mut VecDeque<String>) -> Result<String, String> {
    ensure_layout(root)?;

    let mut thread_id = String::new();
    let mut lines: usize = 20;

    while let Some(flag) = args.pop_front() {
        match flag.as_str() {
            "--thread" => thread_id = pop_required(args, "--thread value")?,
            "--lines" => {
                let value = pop_required(args, "--lines value")?;
                lines = value
                    .parse::<usize>()
                    .map_err(|_| "--lines must be a positive integer".to_string())?;
                if lines == 0 {
                    return Err("--lines must be > 0".to_string());
                }
            }
            _ => return Err(format!("unknown tail option '{flag}'")),
        }
    }

    if thread_id.is_empty() {
        return Err("tail requires --thread".to_string());
    }
    validate_id(&thread_id, "thread_id")?;

    let path = thread_log_path(root, &thread_id);
    if !path.exists() {
        return Err(format!("thread '{}' does not exist", thread_id));
    }

    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    let reader = BufReader::new(file);
    let mut parsed = Vec::new();

    for line in reader.lines() {
        let line = line.map_err(|e| e.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        parsed.push(parse_message_line(&line)?);
    }

    let start = parsed.len().saturating_sub(lines);
    let out: Vec<String> = parsed[start..]
        .iter()
        .map(|m| format!("[{}] {} ({}) {}", m.epoch, m.from, m.kind, m.text))
        .collect();

    if out.is_empty() {
        Ok("(thread empty)".to_string())
    } else {
        Ok(out.join("\n"))
    }
}

fn cmd_task(root: &Path, args: &mut VecDeque<String>) -> Result<String, String> {
    ensure_layout(root)?;
    let sub = pop_required(args, "task subcommand")?;
    match sub.as_str() {
        "create" => task_create(root, args),
        "list" => task_list(root, args),
        "done" => task_done(root, args),
        _ => Err(format!("unknown task subcommand '{sub}'")),
    }
}

fn task_create(root: &Path, args: &mut VecDeque<String>) -> Result<String, String> {
    let mut id = String::new();
    let mut title = String::new();
    let mut desc = String::new();
    let mut owner = String::new();
    let mut why = String::new();
    let mut where_scope = String::new();
    let mut when = String::new();

    while let Some(flag) = args.pop_front() {
        match flag.as_str() {
            "--id" => id = pop_required(args, "--id value")?,
            "--title" => title = pop_required(args, "--title value")?,
            "--desc" => desc = pop_required(args, "--desc value")?,
            "--owner" => owner = pop_required(args, "--owner value")?,
            "--why" => why = pop_required(args, "--why value")?,
            "--where" => where_scope = pop_required(args, "--where value")?,
            "--when" => when = pop_required(args, "--when value")?,
            _ => return Err(format!("unknown task create option '{flag}'")),
        }
    }

    if id.is_empty()
        || title.trim().is_empty()
        || owner.trim().is_empty()
        || why.trim().is_empty()
        || where_scope.trim().is_empty()
        || when.trim().is_empty()
    {
        return Err(
            "task create requires --id, --title, --owner, --why, --where, and --when".to_string(),
        );
    }
    validate_id(&id, "task_id")?;
    validate_registered_agent(root, &owner)?;

    let path = task_open_path(root, &id);
    if path.exists() || task_done_path(root, &id).exists() {
        return Err(format!("task '{}' already exists", id));
    }

    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    writeln!(file, "id={id}").map_err(|e| e.to_string())?;
    writeln!(file, "status=open").map_err(|e| e.to_string())?;
    writeln!(file, "created_epoch={}", now_epoch_secs()).map_err(|e| e.to_string())?;
    writeln!(file, "title={}", escape_field(title.trim())).map_err(|e| e.to_string())?;
    if !desc.trim().is_empty() {
        writeln!(file, "desc={}", escape_field(desc.trim())).map_err(|e| e.to_string())?;
    }
    if !owner.trim().is_empty() {
        writeln!(file, "owner={}", escape_field(owner.trim())).map_err(|e| e.to_string())?;
    }
    writeln!(file, "why={}", escape_field(why.trim())).map_err(|e| e.to_string())?;
    writeln!(file, "where={}", escape_field(where_scope.trim())).map_err(|e| e.to_string())?;
    writeln!(file, "when={}", escape_field(when.trim())).map_err(|e| e.to_string())?;

    Ok(format!("created task '{}'", id))
}

fn task_list(root: &Path, args: &mut VecDeque<String>) -> Result<String, String> {
    let mut status = "open".to_string();
    while let Some(flag) = args.pop_front() {
        match flag.as_str() {
            "--status" => status = pop_required(args, "--status value")?,
            _ => return Err(format!("unknown task list option '{flag}'")),
        }
    }

    if !matches!(status.as_str(), "open" | "done" | "all") {
        return Err("--status must be open, done, or all".to_string());
    }

    let mut lines = Vec::new();
    if status == "open" || status == "all" {
        lines.extend(list_tasks_in(root.join("tasks/open"), "open")?);
    }
    if status == "done" || status == "all" {
        lines.extend(list_tasks_in(root.join("tasks/done"), "done")?);
    }

    if lines.is_empty() {
        Ok("no tasks".to_string())
    } else {
        lines.sort();
        Ok(lines.join("\n"))
    }
}

fn task_done(root: &Path, args: &mut VecDeque<String>) -> Result<String, String> {
    let mut id = String::new();
    let mut by = String::new();
    let mut note = String::new();

    while let Some(flag) = args.pop_front() {
        match flag.as_str() {
            "--id" => id = pop_required(args, "--id value")?,
            "--by" => by = pop_required(args, "--by value")?,
            "--note" => note = pop_required(args, "--note value")?,
            _ => return Err(format!("unknown task done option '{flag}'")),
        }
    }

    if id.is_empty() || by.is_empty() {
        return Err("task done requires --id and --by".to_string());
    }
    validate_id(&id, "task_id")?;
    validate_registered_agent(root, &by)?;

    let open_path = task_open_path(root, &id);
    if !open_path.exists() {
        return Err(format!("open task '{}' not found", id));
    }

    let done_path = task_done_path(root, &id);
    let mut contents = fs::read_to_string(&open_path).map_err(|e| e.to_string())?;
    contents.push_str(&format!("completed_epoch={}\n", now_epoch_secs()));
    contents.push_str(&format!("completed_by={}\n", escape_field(&by)));
    if !note.trim().is_empty() {
        contents.push_str(&format!("completion_note={}\n", escape_field(note.trim())));
    }
    contents.push_str("status=done\n");

    fs::write(&done_path, contents).map_err(|e| e.to_string())?;
    fs::remove_file(open_path).map_err(|e| e.to_string())?;
    Ok(format!("completed task '{}'", id))
}

fn list_tasks_in(dir: PathBuf, status: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("task") {
            continue;
        }
        let raw = fs::read_to_string(&path).map_err(|e| e.to_string())?;
        let id = read_field(&raw, "id").unwrap_or_else(|| "unknown".to_string());
        let title = read_field(&raw, "title")
            .map(|v| unescape_field(&v).unwrap_or(v))
            .unwrap_or_default();
        let owner = read_field(&raw, "owner")
            .map(|v| unescape_field(&v).unwrap_or(v))
            .unwrap_or_else(|| "unassigned".to_string());
        let why = read_field(&raw, "why")
            .map(|v| unescape_field(&v).unwrap_or(v))
            .unwrap_or_default();
        let where_scope = read_field(&raw, "where")
            .map(|v| unescape_field(&v).unwrap_or(v))
            .unwrap_or_default();
        let when = read_field(&raw, "when")
            .map(|v| unescape_field(&v).unwrap_or(v))
            .unwrap_or_default();
        out.push(format!(
            "[{status}] {id} | who={owner} | when={when} | where={where_scope} | why={why} | {title}"
        ));
    }
    Ok(out)
}

fn validate_registered_agent(root: &Path, agent: &str) -> Result<(), String> {
    let agents_path = root.join("agents.txt");
    if !agents_path.exists() {
        return Ok(());
    }

    let content = fs::read_to_string(agents_path).map_err(|e| e.to_string())?;
    let registered: Vec<String> = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    if registered.is_empty() {
        return Ok(());
    }

    if registered.iter().any(|item| item == agent) {
        Ok(())
    } else {
        Err(format!(
            "agent '{}' is not registered (registered: {})",
            agent,
            registered.join(",")
        ))
    }
}

fn validate_kind(kind: &str) -> Result<(), String> {
    match kind {
        "status" | "task" | "question" | "answer" | "decision" => Ok(()),
        _ => Err("--kind must be one of: status, task, question, answer, decision".to_string()),
    }
}

fn parse_agents_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn ensure_layout(root: &Path) -> Result<(), String> {
    fs::create_dir_all(root.join("threads")).map_err(|e| e.to_string())?;
    fs::create_dir_all(root.join("tasks/open")).map_err(|e| e.to_string())?;
    fs::create_dir_all(root.join("tasks/done")).map_err(|e| e.to_string())?;
    Ok(())
}

fn ensure_chat_database(root: &Path) -> Result<(), String> {
    let path = chat_database_path(root);
    let needs_header = !path.exists() || fs::metadata(&path).map_err(|e| e.to_string())?.len() == 0;
    if !needs_header {
        return Ok(());
    }

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    writeln!(file, "# Buddy Chat Database").map_err(|e| e.to_string())?;
    writeln!(file).map_err(|e| e.to_string())?;
    writeln!(file, "| epoch | thread | from | kind | text |").map_err(|e| e.to_string())?;
    writeln!(file, "| --- | --- | --- | --- | --- |").map_err(|e| e.to_string())?;
    Ok(())
}

fn ensure_plan_template(root: &Path) -> Result<(), String> {
    let path = root.join(PLAN_FILE);
    let needs_template =
        !path.exists() || fs::metadata(&path).map_err(|e| e.to_string())?.len() == 0;
    if !needs_template {
        return Ok(());
    }

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    writeln!(file, "# Buddy Execution Plan").map_err(|e| e.to_string())?;
    writeln!(file).map_err(|e| e.to_string())?;
    writeln!(
        file,
        "Nonstop loop rule: keep iterating until no open tasks remain or a hard blocker is recorded."
    )
    .map_err(|e| e.to_string())?;
    writeln!(file).map_err(|e| e.to_string())?;
    writeln!(file, "| task_id | who | why | where | when | status |").map_err(|e| e.to_string())?;
    writeln!(file, "| --- | --- | --- | --- | --- | --- |").map_err(|e| e.to_string())?;
    Ok(())
}

fn append_chat_database_message(
    root: &Path,
    epoch: u64,
    thread_id: &str,
    from: &str,
    kind: &str,
    text: &str,
) -> Result<(), String> {
    ensure_chat_database(root)?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(chat_database_path(root))
        .map_err(|e| e.to_string())?;
    writeln!(
        file,
        "| {} | {} | {} | {} | {} |",
        epoch,
        markdown_cell(thread_id),
        markdown_cell(from),
        markdown_cell(kind),
        markdown_cell(text)
    )
    .map_err(|e| e.to_string())
}

fn markdown_cell(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace('\n', "<br>")
}

fn thread_log_path(root: &Path, thread_id: &str) -> PathBuf {
    root.join("threads").join(format!("{thread_id}.log"))
}

fn chat_database_path(root: &Path) -> PathBuf {
    root.join(CHAT_DATABASE_FILE)
}

fn thread_meta_path(root: &Path, thread_id: &str) -> PathBuf {
    root.join("threads").join(format!("{thread_id}.meta"))
}

fn task_open_path(root: &Path, task_id: &str) -> PathBuf {
    root.join("tasks/open").join(format!("{task_id}.task"))
}

fn task_done_path(root: &Path, task_id: &str) -> PathBuf {
    root.join("tasks/done").join(format!("{task_id}.task"))
}

fn validate_id(value: &str, field: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{field} cannot be empty"));
    }
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.')
    {
        Ok(())
    } else {
        Err(format!(
            "{field} must use [A-Za-z0-9._-] only (got '{}')",
            value
        ))
    }
}

fn pop_required(args: &mut VecDeque<String>, label: &str) -> Result<String, String> {
    args.pop_front().ok_or_else(|| format!("missing {label}"))
}

fn now_epoch_secs() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MessageLine {
    epoch: u64,
    from: String,
    kind: String,
    text: String,
}

fn parse_message_line(line: &str) -> Result<MessageLine, String> {
    let parts: Vec<&str> = line.split('\t').collect();
    if parts.len() != 4 {
        return Err("invalid message line format".to_string());
    }
    let epoch = parts[0]
        .parse::<u64>()
        .map_err(|_| "invalid message epoch".to_string())?;
    Ok(MessageLine {
        epoch,
        from: unescape_field(parts[1])?,
        kind: unescape_field(parts[2])?,
        text: unescape_field(parts[3])?,
    })
}

fn read_field(raw: &str, key: &str) -> Option<String> {
    for line in raw.lines() {
        if let Some((k, v)) = line.split_once('=')
            && k.trim() == key
        {
            return Some(v.to_string());
        }
    }
    None
}

fn escape_field(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
}

fn unescape_field(value: &str) -> Result<String, String> {
    let mut output = String::with_capacity(value.len());
    let mut escaped = false;
    for ch in value.chars() {
        if escaped {
            match ch {
                '\\' => output.push('\\'),
                't' => output.push('\t'),
                'n' => output.push('\n'),
                other => return Err(format!("invalid escape sequence: \\{other}")),
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            output.push(ch);
        }
    }
    if escaped {
        return Err("unterminated escape sequence".to_string());
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("buddy-chat-{label}-{}-{nanos}", std::process::id()));
        path
    }

    #[test]
    fn escape_round_trip() {
        let raw = "hello\\nworld\t1\\2";
        let escaped = escape_field(raw);
        assert_eq!(raw, unescape_field(&escaped).unwrap());
    }

    #[test]
    fn init_thread_send_tail_round_trip() {
        let root = test_root("roundtrip");
        let root_s = root.to_string_lossy().to_string();

        run(vec![
            "buddy-chat".into(),
            "--root".into(),
            root_s.clone(),
            "init".into(),
            "--agents".into(),
            "codex,buddy".into(),
        ])
        .unwrap();

        run(vec![
            "buddy-chat".into(),
            "--root".into(),
            root_s.clone(),
            "thread".into(),
            "create".into(),
            "standup".into(),
        ])
        .unwrap();

        run(vec![
            "buddy-chat".into(),
            "--root".into(),
            root_s.clone(),
            "send".into(),
            "--thread".into(),
            "standup".into(),
            "--from".into(),
            "codex".into(),
            "--kind".into(),
            "status".into(),
            "--text".into(),
            "checkpoint ready".into(),
        ])
        .unwrap();

        let tail = run(vec![
            "buddy-chat".into(),
            "--root".into(),
            root_s.clone(),
            "tail".into(),
            "--thread".into(),
            "standup".into(),
        ])
        .unwrap();
        assert!(tail.contains("codex (status) checkpoint ready"));
        let chat_db = fs::read_to_string(root.join(CHAT_DATABASE_FILE)).unwrap();
        assert!(chat_db.contains("| thread | from | kind | text |"));
        assert!(chat_db.contains("| standup | codex | status | checkpoint ready |"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn task_lifecycle_works() {
        let root = test_root("tasks");
        let root_s = root.to_string_lossy().to_string();

        run(vec![
            "buddy-chat".into(),
            "--root".into(),
            root_s.clone(),
            "init".into(),
            "--agents".into(),
            "codex,buddy".into(),
        ])
        .unwrap();

        run(vec![
            "buddy-chat".into(),
            "--root".into(),
            root_s.clone(),
            "task".into(),
            "create".into(),
            "--id".into(),
            "t1".into(),
            "--title".into(),
            "Build transport".into(),
            "--owner".into(),
            "buddy".into(),
            "--why".into(),
            "Need explicit agent ownership".into(),
            "--where".into(),
            "services/retrieval".into(),
            "--when".into(),
            "current-milestone".into(),
        ])
        .unwrap();

        let open_list = run(vec![
            "buddy-chat".into(),
            "--root".into(),
            root_s.clone(),
            "task".into(),
            "list".into(),
            "--status".into(),
            "open".into(),
        ])
        .unwrap();
        assert!(open_list.contains("[open] t1"));
        assert!(open_list.contains("who=buddy"));
        assert!(open_list.contains("when=current-milestone"));

        run(vec![
            "buddy-chat".into(),
            "--root".into(),
            root_s.clone(),
            "task".into(),
            "done".into(),
            "--id".into(),
            "t1".into(),
            "--by".into(),
            "codex".into(),
        ])
        .unwrap();

        let done_list = run(vec![
            "buddy-chat".into(),
            "--root".into(),
            root_s.clone(),
            "task".into(),
            "list".into(),
            "--status".into(),
            "done".into(),
        ])
        .unwrap();
        assert!(done_list.contains("[done] t1"));

        let _ = fs::remove_dir_all(root);
    }
}
