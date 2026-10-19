use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

use crate::{
    VERSION,
    engine::{DownloadRequest, Engine, Progress, safe_output_name},
    logging::LogSink,
};

#[derive(Clone)]
struct Task {
    status: String,
    total: u64,
    completed: u64,
    speed: u64,
    error: Option<String>,
    cancel: Arc<AtomicBool>,
    request: DownloadRequest,
}

#[derive(Clone)]
pub struct RpcServer {
    engine: Engine,
    log: Arc<LogSink>,
    secret: String,
    tasks: Arc<RwLock<HashMap<String, Task>>>,
}

impl RpcServer {
    pub fn new(engine: Engine, log: Arc<LogSink>, secret: String) -> Self {
        Self {
            engine,
            log,
            secret,
            tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn serve(self, port: u16) -> Result<()> {
        let listener = TcpListener::bind(("127.0.0.1", port)).await?;
        let addr = listener.local_addr()?;
        self.log
            .info("rpc.started", &format!("listen=127.0.0.1:{}", addr.port()));
        loop {
            let (stream, peer) = listener.accept().await?;
            if !peer.ip().is_loopback() {
                continue;
            }
            let server = self.clone();
            tokio::spawn(async move {
                if let Err(error) = server.handle(stream).await {
                    server.log.warn("rpc.request_failed", &error.to_string());
                }
            });
        }
    }

    async fn handle(&self, mut stream: TcpStream) -> Result<()> {
        let mut buffer = vec![0u8; 1024 * 1024];
        let mut used = 0usize;
        let header_end = loop {
            if used == buffer.len() {
                return Err(anyhow!("request too large"));
            }
            let n = stream.read(&mut buffer[used..]).await?;
            if n == 0 {
                return Err(anyhow!("connection closed"));
            }
            used += n;
            if let Some(pos) = find_bytes(&buffer[..used], b"\r\n\r\n") {
                break pos + 4;
            }
        };

        let headers = std::str::from_utf8(&buffer[..header_end])?;
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.strip_prefix("Content-Length:")
                    .or_else(|| line.strip_prefix("content-length:"))
            })
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        if content_length > 1024 * 1024 {
            return Err(anyhow!("RPC body too large"));
        }
        while used < header_end + content_length {
            let n = stream.read(&mut buffer[used..]).await?;
            if n == 0 {
                return Err(anyhow!("connection closed"));
            }
            used += n;
        }

        let body = &buffer[header_end..header_end + content_length];
        let request: Value = serde_json::from_slice(body).context("invalid JSON-RPC body")?;
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing method"))?;
        let mut params = request
            .get("params")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        if !self.secret.is_empty() {
            let token = params
                .first()
                .and_then(Value::as_str)
                .and_then(|v| v.strip_prefix("token:"));
            if token != Some(self.secret.as_str()) {
                return self
                    .write_json(&mut stream, json!({"jsonrpc":"2.0","id":id,"error":{"code":-32001,"message":"unauthorized"}}))
                    .await;
            }
            params.remove(0);
        }

        let result = self.dispatch(method, params).await;
        let payload = match result {
            Ok(value) => json!({"jsonrpc":"2.0","id":id,"result":value}),
            Err(error) => {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":error.to_string()}})
            }
        };
        self.write_json(&mut stream, payload).await
    }

    async fn write_json(&self, stream: &mut TcpStream, payload: Value) -> Result<()> {
        let body = payload.to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await?;
        Ok(())
    }

    async fn dispatch(&self, method: &str, params: Vec<Value>) -> Result<Value> {
        match method {
            "aria2.getVersion" | "nibiru.getVersion" => Ok(json!({
                "version": VERSION,
                "enabledFeatures": ["HTTP", "HTTPS", "HTTP/2", "RPC", "Resume"]
            })),
            "aria2.addUri" | "nibiru.addUri" => self.add_uri(params).await,
            "aria2.tellStatus" | "nibiru.tellStatus" => self.tell_status(params).await,
            "aria2.pause" | "aria2.forcePause" | "nibiru.pause" => self.pause(params).await,
            "aria2.unpause" | "nibiru.resume" => self.resume(params).await,
            "aria2.remove" | "aria2.forceRemove" | "nibiru.remove" => self.remove(params).await,
            "aria2.removeDownloadResult" => Ok(Value::String("OK".into())),
            "aria2.changeOption" | "aria2.changeGlobalOption" => Ok(Value::String("OK".into())),
            _ => Err(anyhow!("method not implemented: {method}")),
        }
    }

    async fn add_uri(&self, params: Vec<Value>) -> Result<Value> {
        let uri = params
            .first()
            .and_then(Value::as_array)
            .and_then(|v| v.first())
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing URI"))?
            .to_owned();
        let options = params.get(1).and_then(Value::as_object);
        let dir = options
            .and_then(|m| m.get("dir"))
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        let out = options
            .and_then(|m| m.get("out"))
            .and_then(Value::as_str)
            .unwrap_or("download");
        if !safe_output_name(out) {
            return Err(anyhow!("invalid output filename"));
        }
        let request = DownloadRequest {
            url: uri,
            destination: dir.join(out),
        };
        let gid = uuid::Uuid::new_v4().simple().to_string();
        let cancel = Arc::new(AtomicBool::new(false));
        self.tasks.write().await.insert(
            gid.clone(),
            Task {
                status: "active".into(),
                total: 0,
                completed: 0,
                speed: 0,
                error: None,
                cancel: cancel.clone(),
                request: request.clone(),
            },
        );
        self.spawn_download(gid.clone(), request, cancel);
        Ok(Value::String(gid))
    }

    fn spawn_download(&self, gid: String, request: DownloadRequest, cancel: Arc<AtomicBool>) {
        let engine = self.engine.clone();
        let tasks = self.tasks.clone();
        let log = self.log.clone();
        tokio::spawn(async move {
            let gid_progress = gid.clone();
            let tasks_progress = tasks.clone();
            let result = engine
                .download(
                    request.clone(),
                    cancel.clone(),
                    move |Progress {
                              downloaded,
                              total,
                              speed_bytes_per_second,
                          }| {
                        let tasks = tasks_progress.clone();
                        let gid = gid_progress.clone();
                        tokio::spawn(async move {
                            if let Some(task) = tasks.write().await.get_mut(&gid) {
                                task.completed = downloaded;
                                task.total = total.unwrap_or_default();
                                task.speed = speed_bytes_per_second;
                            }
                        });
                    },
                )
                .await;
            let mut guard = tasks.write().await;
            if let Some(task) = guard.get_mut(&gid) {
                match result {
                    Ok(()) => {
                        task.status = "complete".into();
                        task.speed = 0;
                        if task.total == 0 {
                            task.total = task.completed;
                        }
                    }
                    Err(_error) if cancel.load(Ordering::Acquire) => {
                        task.status = "paused".into();
                        task.speed = 0;
                    }
                    Err(error) => {
                        task.status = "error".into();
                        task.speed = 0;
                        task.error = Some(error.to_string());
                        log.error("rpc.download_failed", &format!("gid={gid} error={error}"));
                    }
                }
            }
        });
    }

    async fn tell_status(&self, params: Vec<Value>) -> Result<Value> {
        let gid = params
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing gid"))?;
        let guard = self.tasks.read().await;
        let task = guard.get(gid).ok_or_else(|| anyhow!("unknown gid"))?;
        Ok(json!({
            "gid": gid,
            "status": task.status,
            "totalLength": task.total.to_string(),
            "completedLength": task.completed.to_string(),
            "downloadSpeed": task.speed.to_string(),
            "uploadSpeed": "0",
            "connections": if task.status == "active" { "1" } else { "0" },
            "errorMessage": task.error.clone().unwrap_or_default(),
            "files": [{
                "index":"1",
                "path":task.request.destination.to_string_lossy(),
                "length":task.total.to_string(),
                "completedLength":task.completed.to_string(),
                "selected":"true",
                "uris":[{"uri":task.request.url,"status":if task.status=="active"{"used"}else{"waiting"}}]
            }]
        }))
    }

    async fn pause(&self, params: Vec<Value>) -> Result<Value> {
        let gid = params.first().and_then(Value::as_str).ok_or_else(|| anyhow!("missing gid"))?;
        let guard = self.tasks.read().await;
        let task = guard.get(gid).ok_or_else(|| anyhow!("unknown gid"))?;
        task.cancel.store(true, Ordering::Release);
        Ok(Value::String(gid.to_owned()))
    }

    async fn resume(&self, params: Vec<Value>) -> Result<Value> {
        let gid = params
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing gid"))?
            .to_owned();
        let (request, cancel) = {
            let mut guard = self.tasks.write().await;
            let task = guard.get_mut(&gid).ok_or_else(|| anyhow!("unknown gid"))?;
            if task.status != "paused" && task.status != "error" {
                return Ok(Value::String(gid));
            }
            let cancel = Arc::new(AtomicBool::new(false));
            task.cancel = cancel.clone();
            task.status = "active".into();
            task.error = None;
            (task.request.clone(), cancel)
        };
        self.spawn_download(gid.clone(), request, cancel);
        Ok(Value::String(gid))
    }

    async fn remove(&self, params: Vec<Value>) -> Result<Value> {
        let gid = params.first().and_then(Value::as_str).ok_or_else(|| anyhow!("missing gid"))?.to_owned();
        if let Some(task) = self.tasks.write().await.remove(&gid) {
            task.cancel.store(true, Ordering::Release);
        }
        Ok(Value::String(gid))
    }
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
