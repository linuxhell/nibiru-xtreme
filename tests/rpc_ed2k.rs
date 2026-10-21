//! `ed2k://` tasks through the real JSON-RPC server, downloading from a Nibiru ED2K listener
//! over loopback, with the task table persisted and restored like any other task.
//! The uploader is this project's own code, so this proves the RPC/task/session behaviour,
//! not interoperability with aMule/eMule.

use nibiru_xtreme::{
    ed2k::{FileLink, IncomingListener, ShareRegistry, UploadConfig, UploadManager},
    engine::Engine,
    logging::LogSink,
    rpc::{
        RpcServer,
        ed2k::{Ed2kConfig, Ed2kService},
    },
};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};

fn sample(len: usize) -> Vec<u8> {
    let mut x = 0x9e3779b97f4a7c15u64;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

struct Node {
    port: u16,
    client: reqwest::Client,
}

async fn node(session: &Path, downloads: &Path) -> Node {
    let log = Arc::new(LogSink::new(None).unwrap());
    let ed2k = Ed2kService::new(Ed2kConfig {
        default_dir: downloads.to_owned(),
        identity: [9; 16],
        tcp_port: 0,
        servers: Vec::new(),
        kad: None,
        obfuscation: nibiru_xtreme::ed2k::obfuscation::Obfuscation::Prefer,
    });
    let server = RpcServer::new(Engine::new(log.clone()).unwrap(), log, "k".into())
        .with_ed2k(ed2k)
        .with_session(session.to_owned())
        .await
        .unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let running = server.clone();
    tokio::spawn(async move { running.serve(port).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    Node {
        port,
        client: reqwest::Client::new(),
    }
}

impl Node {
    async fn try_call(&self, method: &str, params: Vec<Value>) -> Result<Value, String> {
        let mut all = vec![json!("token:k")];
        all.extend(params);
        let response: Value = self
            .client
            .post(format!("http://127.0.0.1:{}/jsonrpc", self.port))
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":all}))
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        match response.get("error") {
            Some(e) => Err(e["message"].as_str().unwrap_or("error").to_owned()),
            None => Ok(response["result"].clone()),
        }
    }
    async fn call(&self, method: &str, params: Vec<Value>) -> Value {
        self.try_call(method, params).await.unwrap()
    }
    async fn wait(&self, gid: &str, wanted: &str) -> Value {
        for _ in 0..400 {
            let status = self.call("nibiru.tellStatus", vec![json!(gid)]).await;
            if status["status"] == wanted {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("never reached {wanted}");
    }
}

#[tokio::test]
async fn ed2k_links_download_through_rpc_and_unfinished_tasks_survive_a_restart() {
    let shared = tempfile::tempdir().unwrap();
    let path = shared.path().join("shared.bin");
    let bytes = sample(10_000_000); // two ED2K parts: exercises the hashset exchange
    std::fs::write(&path, &bytes).unwrap();
    let shares = ShareRegistry::default();
    let info = shares.add_file(path).await.unwrap();
    let listener = IncomingListener::bind_with_uploads(
        [5; 16],
        0,
        shares,
        UploadManager::new(UploadConfig::default()),
    )
    .await
    .unwrap();
    let source_port = listener.status().tcp_port;
    let link = FileLink::parse(&info.link).unwrap().link();

    let work = tempfile::tempdir().unwrap();
    let downloads = work.path().join("downloads");
    std::fs::create_dir_all(&downloads).unwrap();
    let session = work.path().join("session.json");

    let first = node(&session, &downloads).await;
    let version = first.call("nibiru.getVersion", vec![]).await;
    assert!(
        version["enabledFeatures"]
            .as_array()
            .unwrap()
            .contains(&json!("ED2K"))
    );

    // 1. Nothing can serve this: the task errors out, and is remembered as the same GID.
    let dead = first
        .call(
            "nibiru.addUri",
            vec![
                json!([link]),
                json!({"x-nibiru-ed2k-source": "127.0.0.1:9"}),
            ],
        )
        .await;
    let gid = dead.as_str().unwrap().to_owned();
    first.wait(&gid, "error").await;
    for _ in 0..100 {
        if std::fs::read_to_string(&session).is_ok_and(|s| s.contains(&gid)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let saved = std::fs::read_to_string(&session).unwrap();
    assert!(
        saved.contains("\"ed2k\"") && saved.contains(&gid),
        "{saved}"
    );

    // 2. A fresh engine restores it paused, never silently dropped or completed. A new task for the
    //    same link with a live source then downloads normally.
    let second = node(&session, &downloads).await;
    let restored = second.call("nibiru.tellStatus", vec![json!(gid)]).await;
    assert_eq!(restored["status"], "paused");
    assert_eq!(
        second.call("nibiru.getSessionInfo", vec![]).await["restored"],
        1
    );
    let waiting = second
        .call("nibiru.tellWaiting", vec![json!(0), json!(10)])
        .await;
    assert_eq!(waiting.as_array().unwrap().len(), 1);

    let live = second
        .call(
            "nibiru.addUri",
            vec![
                json!([link]),
                json!({"x-nibiru-ed2k-source": format!("127.0.0.1:{source_port}")}),
            ],
        )
        .await;
    let done = second.wait(live.as_str().unwrap(), "complete").await;
    assert_eq!(done["completedLength"], bytes.len().to_string());
    assert_eq!(std::fs::read(downloads.join(&info.name)).unwrap(), bytes);
    let stopped = second
        .call("nibiru.tellStopped", vec![json!(0), json!(10)])
        .await;
    assert_eq!(stopped.as_array().unwrap().len(), 1);

    // 3. Removing the stale entry works and clears it from the session.
    second.call("nibiru.remove", vec![json!(gid)]).await;
    second.call("nibiru.removeResult", vec![json!(gid)]).await;
    let stat = second.call("nibiru.getGlobalStat", vec![]).await;
    assert_eq!(stat["numActive"], "0");
    assert!(
        second
            .try_call("nibiru.tellStatus", vec![json!(gid)])
            .await
            .is_err()
    );
}
