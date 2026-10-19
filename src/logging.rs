use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

pub struct LogSink {
    file: Option<Mutex<File>>,
}

impl LogSink {
    pub fn new(path: Option<&Path>) -> std::io::Result<Self> {
        let file = match path {
            Some(path) => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                Some(Mutex::new(
                    OpenOptions::new().create(true).append(true).open(path)?,
                ))
            }
            None => None,
        };
        Ok(Self { file })
    }

    pub fn info(&self, event: &str, detail: &str) {
        self.write("INFO", event, detail);
    }

    pub fn warn(&self, event: &str, detail: &str) {
        self.write("WARN", event, detail);
    }

    pub fn error(&self, event: &str, detail: &str) {
        self.write("ERROR", event, detail);
    }

    fn write(&self, level: &str, event: &str, detail: &str) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|v| v.as_secs_f64())
            .unwrap_or_default();
        let record = serde_json::json!({
            "ts": ts,
            "level": level,
            "event": event,
            "detail": detail,
        });
        let line = record.to_string();
        eprintln!("{line}");
        if let Some(file) = &self.file {
            if let Ok(mut file) = file.lock() {
                let _ = writeln!(file, "{line}");
            }
        }
    }
}
