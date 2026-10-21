//! `nibiru-xtreme diagnose`: finds out why a download does not hold its top speed from start to
//! finish by measuring the pieces separately: the network alone (no disk), the disk alone, and
//! the engine on the same link, then comparing them.

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use reqwest::{
    StatusCode,
    header::{CONTENT_RANGE, RANGE},
};
use std::{
    fmt::Write as _,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use url::Url;

use crate::{
    engine::{DownloadRequest, Engine},
    logging::LogSink,
    storage,
};

const SPAN: u64 = 32 * 1024 * 1024;
const MB: f64 = 1_000_000.0;

pub struct Options {
    pub url: String,
    pub dir: PathBuf,
    pub seconds: u64,
    pub connections: Vec<usize>,
    pub disk_mib: u64,
    pub skip_disk: bool,
    pub engine_seconds: u64,
    pub engine_connections: Option<usize>,
    pub full: bool,
    pub report: PathBuf,
}

/// Bytes per second, one value per elapsed second.
#[derive(Default, Clone)]
struct Series(Vec<f64>);

impl Series {
    fn from_cumulative(samples: &[(f64, u64)]) -> Self {
        // `samples` are (elapsed seconds, cumulative bytes); interpolate at whole seconds.
        let mut out = Vec::new();
        let Some(last) = samples.last() else {
            return Self(out);
        };
        let mut previous = 0_u64;
        for second in 1..=(last.0.floor() as u64) {
            let at = second as f64;
            let bytes = value_at(samples, at);
            out.push(bytes.saturating_sub(previous) as f64);
            previous = bytes;
        }
        Self(out)
    }

    fn mb(&self) -> Vec<f64> {
        self.0.iter().map(|v| v / MB).collect()
    }

    /// Seconds before the speed first reaches 90% of the steady median.
    fn ramp_seconds(&self) -> Option<usize> {
        let steady = self.steady_median()?;
        self.mb().iter().position(|v| *v >= steady * 0.9)
    }

    fn steady_median(&self) -> Option<f64> {
        let mut values = self.mb();
        if values.len() < 4 {
            return None;
        }
        values.drain(..2.min(values.len()));
        values.sort_by(|a, b| a.total_cmp(b));
        Some(values[values.len() / 2])
    }

    /// Average over the seconds after the first two (the connection ramp-up).
    fn steady_avg(&self) -> f64 {
        let values = self.mb();
        let tail = if values.len() > 3 {
            &values[2..]
        } else {
            &values[..]
        };
        if tail.is_empty() {
            0.0
        } else {
            tail.iter().sum::<f64>() / tail.len() as f64
        }
    }

    fn avg(&self) -> f64 {
        let values = self.mb();
        if values.is_empty() {
            0.0
        } else {
            values.iter().sum::<f64>() / values.len() as f64
        }
    }

    fn percentile(&self, p: f64) -> f64 {
        let mut values = self.mb();
        if values.is_empty() {
            return 0.0;
        }
        values.sort_by(|a, b| a.total_cmp(b));
        values[((values.len() - 1) as f64 * p).round() as usize]
    }

    fn dips(&self) -> Vec<(usize, f64)> {
        let Some(median) = self.steady_median() else {
            return Vec::new();
        };
        self.mb()
            .into_iter()
            .enumerate()
            .skip(2)
            .filter(|(_, v)| *v < median * 0.9)
            .collect()
    }

    fn line(&self) -> String {
        self.mb()
            .iter()
            .map(|v| format!("{v:.0}"))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

fn value_at(samples: &[(f64, u64)], at: f64) -> u64 {
    let mut before = (0.0, 0_u64);
    for sample in samples {
        if sample.0 > at {
            let span = (sample.0 - before.0).max(1e-9);
            let fraction = (at - before.0) / span;
            return before.1 + ((sample.1.saturating_sub(before.1)) as f64 * fraction) as u64;
        }
        before = *sample;
    }
    before.1
}

struct Report {
    text: String,
}

impl Report {
    fn say(&mut self, line: impl AsRef<str>) {
        println!("{}", line.as_ref());
        let _ = writeln!(self.text, "{}", line.as_ref());
    }
}

pub async fn run(options: Options) -> Result<()> {
    let mut report = Report {
        text: String::new(),
    };
    let url = Url::parse(&options.url).context("invalid URL")?;
    tokio::fs::create_dir_all(&options.dir).await?;
    report.say(format!(
        "nibiru-xtreme {} diagnose · {} {} · pasta de teste: {}",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        options.dir.display()
    ));

    let engine_log = options.dir.join("nibiru-diagnose-engine.jsonl");
    let _ = tokio::fs::remove_file(&engine_log).await;
    let log = Arc::new(LogSink::file_only(&engine_log)?);
    let mut engine = Engine::new(log.clone())?;
    if let Some(limit) = options.engine_connections {
        engine = engine.with_request_options(crate::engine::RequestOptions {
            max_connections: Some(limit),
            ..Default::default()
        })?;
    }
    let (total, ranges) = engine.probe(&options.url).await?;
    let total = total.context("o servidor não informou o tamanho do arquivo")?;
    report.say(format!(
        "arquivo: {:.2} GB · faixas (Range): {}",
        total as f64 / 1e9,
        if ranges {
            "sim"
        } else {
            "NÃO (sem conexões paralelas)"
        }
    ));
    if !ranges {
        bail!("sem suporte a Range, não dá para comparar conexões");
    }

    // 1. Network alone.
    report.say("");
    report.say(format!(
        "[1/3] REDE SEM DISCO ({} s por teste, bytes descartados)",
        options.seconds
    ));
    let client = Engine::probe_client()?;
    let mut network: Vec<(usize, Series, ProbeStats)> = Vec::new();
    for &connections in &options.connections {
        let (series, stats) = net_probe(&client, &url, total, connections, options.seconds).await?;
        report.say(format!(
            "  {connections:>2} conexões: média {:>6.1} MB/s · após rampa {:>6.1} · mín {:>6.1} · p10 {:>6.1} · rampa {} s · espera de cabeçalho média {:.0} ms (máx {:.0}) · {} requisições",
            series.avg(),
            series.steady_avg(),
            series.percentile(0.0),
            series.percentile(0.1),
            series.ramp_seconds().map_or("?".into(), |s| s.to_string()),
            stats.header_avg_ms,
            stats.header_max_ms,
            stats.requests,
        ));
        report.say(format!("      MB/s por segundo: {}", series.line()));
        if let Some(error) = &stats.error {
            report.say(format!("      aviso: {error}"));
        }
        network.push((connections, series, stats));
        // Let the server forget the burst before the next test.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // 2. Disk alone.
    report.say("");
    let disk = if options.skip_disk {
        report.say("[2/3] DISCO: pulado");
        None
    } else {
        report.say(format!(
            "[2/3] DISCO ({} MiB gravados em blocos de 4 MiB, 8 em paralelo, com descarga final)",
            options.disk_mib
        ));
        let result = disk_test(&options.dir, options.disk_mib).await?;
        report.say(format!(
            "  gravação: {:.0} MB/s sustentados (total {:.1} s, descarga final {:.1} s) · MB/s por segundo: {}",
            result.0, result.2, result.3, result.1.line()
        ));
        Some(result.0)
    };

    // 3. The engine on the same link.
    report.say("");
    let mut mode = if options.full {
        "download completo".to_owned()
    } else {
        format!("primeiros {} s", options.engine_seconds)
    };
    if let Some(n) = options.engine_connections {
        mode.push_str(&format!(", limitado a {n} conexões"));
    }
    report.say(format!("[3/3] MOTOR ({mode})"));
    let destination = options.dir.join("nibiru-diagnose-engine.bin");
    let (series, finished) = engine_run(
        &engine,
        &options.url,
        &destination,
        if options.full {
            None
        } else {
            Some(options.engine_seconds)
        },
    )
    .await;
    for leftover in [
        destination.clone(),
        PathBuf::from(format!("{}.nibiru-http.part", destination.display())),
        PathBuf::from(format!("{}.nibiru-http.part.json", destination.display())),
        PathBuf::from(format!("{}.nibiru-http.json", destination.display())),
        PathBuf::from(format!("{}.nibiru-http.lock", destination.display())),
    ] {
        let _ = tokio::fs::remove_file(leftover).await;
    }
    report.say(format!(
        "  média {:>6.1} MB/s · após rampa {:>6.1} · mín {:>6.1} · p10 {:>6.1} · rampa {} s · {}",
        series.avg(),
        series.steady_avg(),
        series.percentile(0.0),
        series.percentile(0.1),
        series.ramp_seconds().map_or("?".into(), |s| s.to_string()),
        if finished {
            "terminou"
        } else {
            "interrompido no tempo"
        },
    ));
    report.say(format!("      MB/s por segundo: {}", series.line()));
    let metrics = engine_metrics(&engine_log).await;
    if let Some(metrics) = &metrics {
        report.say(format!(
            "  por conexão, do tempo total: cabeçalhos {:.1}% · corpo (rede) {:.1}% · gravação {:.1}% ({} requisições, {} conexões)",
            metrics.header_pct, metrics.body_pct, metrics.storage_pct, metrics.ranges, metrics.workers
        ));
    }

    // Verdict.
    report.say("");
    report.say("VEREDITO");
    verdict(
        &mut report,
        &network,
        disk,
        &series,
        metrics.as_ref(),
        total,
    );

    let _ = tokio::fs::remove_file(&engine_log).await;
    tokio::fs::write(&options.report, report.text.as_bytes()).await?;
    println!("\nrelatório salvo em {}", options.report.display());
    Ok(())
}

struct ProbeStats {
    requests: u64,
    header_avg_ms: f64,
    header_max_ms: f64,
    error: Option<String>,
}

async fn net_probe(
    client: &reqwest::Client,
    url: &Url,
    total: u64,
    connections: usize,
    seconds: u64,
) -> Result<(Series, ProbeStats)> {
    let bytes = Arc::new(AtomicU64::new(0));
    let requests = Arc::new(AtomicU64::new(0));
    let header_us = Arc::new(AtomicU64::new(0));
    let header_max_us = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let error = Arc::new(std::sync::Mutex::new(None::<String>));
    let usable = total.saturating_sub(SPAN).max(1);
    let started = Instant::now();
    let mut tasks = Vec::new();
    for worker in 0..connections.max(1) {
        let (client, url) = (client.clone(), url.clone());
        let (bytes, requests, header_us, header_max_us, stop, error) = (
            bytes.clone(),
            requests.clone(),
            header_us.clone(),
            header_max_us.clone(),
            stop.clone(),
            error.clone(),
        );
        let step = connections.max(1) as u64;
        tasks.push(tokio::spawn(async move {
            let mut turn = worker as u64;
            while !stop.load(Ordering::Acquire) {
                let start = (turn * SPAN) % usable;
                let end = (start + SPAN - 1).min(total - 1);
                turn += step;
                let asked = Instant::now();
                let response = match client
                    .get(url.clone())
                    .header(RANGE, format!("bytes={start}-{end}"))
                    .send()
                    .await
                {
                    Ok(response) => response,
                    Err(failure) => {
                        *error.lock().unwrap() = Some(failure.without_url().to_string());
                        return;
                    }
                };
                let waited = asked.elapsed().as_micros() as u64;
                header_us.fetch_add(waited, Ordering::Relaxed);
                header_max_us.fetch_max(waited, Ordering::Relaxed);
                requests.fetch_add(1, Ordering::Relaxed);
                if response.status() != StatusCode::PARTIAL_CONTENT
                    || response.headers().get(CONTENT_RANGE).is_none()
                {
                    *error.lock().unwrap() = Some(format!(
                        "o servidor respondeu {} a uma faixa",
                        response.status()
                    ));
                    return;
                }
                let mut body = response.bytes_stream();
                while let Some(chunk) = body.next().await {
                    let Ok(chunk) = chunk else { return };
                    bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    if stop.load(Ordering::Acquire) {
                        return;
                    }
                }
            }
        }));
    }
    let mut samples = vec![(0.0, 0_u64)];
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    while started.elapsed() < Duration::from_secs(seconds) {
        ticker.tick().await;
        samples.push((
            started.elapsed().as_secs_f64(),
            bytes.load(Ordering::Relaxed),
        ));
        if error.lock().unwrap().is_some() && tasks.iter().all(|t| t.is_finished()) {
            break;
        }
    }
    stop.store(true, Ordering::Release);
    for task in tasks {
        let _ = task.await;
    }
    let count = requests.load(Ordering::Relaxed).max(1);
    Ok((
        Series::from_cumulative(&samples),
        ProbeStats {
            requests: requests.load(Ordering::Relaxed),
            header_avg_ms: header_us.load(Ordering::Relaxed) as f64 / count as f64 / 1000.0,
            header_max_ms: header_max_us.load(Ordering::Relaxed) as f64 / 1000.0,
            error: error.lock().unwrap().clone(),
        },
    ))
}

/// Returns (sustained MB/s, per-second series, total seconds, final flush seconds).
async fn disk_test(dir: &Path, mib: u64) -> Result<(f64, Series, f64, f64)> {
    let path = dir.join("nibiru-diagnose-disk.tmp");
    let file = Arc::new(
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?,
    );
    let block = 4 * 1024 * 1024_u64;
    let blocks = (mib * 1024 * 1024 / block).max(1);
    file.set_len(blocks * block)?;
    let written = Arc::new(AtomicU64::new(0));
    let next = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut workers = Vec::new();
    for _ in 0..8 {
        let (file, written, next) = (file.clone(), written.clone(), next.clone());
        workers.push(tokio::spawn(async move {
            let mut pattern = vec![0_u8; block as usize];
            let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
            loop {
                let index = next.fetch_add(1, Ordering::AcqRel);
                if index >= blocks {
                    return Ok::<(), anyhow::Error>(());
                }
                for chunk in pattern.chunks_mut(8) {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    let bytes = seed.to_le_bytes();
                    chunk.copy_from_slice(&bytes[..chunk.len()]);
                }
                storage::write_at(file.clone(), index * block, pattern.clone()).await?;
                written.fetch_add(block, Ordering::Relaxed);
            }
        }));
    }
    let mut samples = vec![(0.0, 0_u64)];
    while workers.iter().any(|w| !w.is_finished()) {
        tokio::time::sleep(Duration::from_millis(250)).await;
        samples.push((
            started.elapsed().as_secs_f64(),
            written.load(Ordering::Relaxed),
        ));
    }
    for worker in workers {
        worker.await??;
    }
    let written_at = started.elapsed().as_secs_f64();
    let flush = Instant::now();
    let flushing = file.clone();
    tokio::task::spawn_blocking(move || flushing.sync_all()).await??;
    let flush_s = flush.elapsed().as_secs_f64();
    let total_s = started.elapsed().as_secs_f64();
    drop(file);
    let _ = tokio::fs::remove_file(&path).await;
    let _ = written_at;
    Ok((
        (blocks * block) as f64 / total_s / MB,
        Series::from_cumulative(&samples),
        total_s,
        flush_s,
    ))
}

/// Runs the engine for `limit` seconds (or to the end) and returns its speed per second.
async fn engine_run(
    engine: &Engine,
    url: &str,
    destination: &Path,
    limit: Option<u64>,
) -> (Series, bool) {
    let cancelled = Arc::new(AtomicBool::new(false));
    let samples = Arc::new(std::sync::Mutex::new(vec![(0.0_f64, 0_u64)]));
    let started = Instant::now();
    let timer = limit.map(|seconds| {
        let cancelled = cancelled.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(seconds)).await;
            cancelled.store(true, Ordering::Release);
        })
    });
    let recorder = samples.clone();
    let result = engine
        .download(
            DownloadRequest {
                url: url.to_owned(),
                destination: destination.to_owned(),
            },
            cancelled,
            move |progress| {
                recorder
                    .lock()
                    .unwrap()
                    .push((started.elapsed().as_secs_f64(), progress.downloaded));
            },
        )
        .await;
    if let Some(timer) = timer {
        timer.abort();
    }
    let samples = samples.lock().unwrap().clone();
    (Series::from_cumulative(&samples), result.is_ok())
}

struct EngineMetrics {
    header_pct: f64,
    body_pct: f64,
    storage_pct: f64,
    ranges: u64,
    workers: u64,
}

async fn engine_metrics(log: &Path) -> Option<EngineMetrics> {
    let text = tokio::fs::read_to_string(log).await.ok()?;
    let field = |detail: &str, key: &str| -> Option<f64> {
        detail
            .split_whitespace()
            .find_map(|part| part.strip_prefix(&format!("{key}=")))?
            .parse()
            .ok()
    };
    let mut workers = None;
    let mut metrics = None;
    for line in text.lines() {
        let value: serde_json::Value = serde_json::from_str(line).ok()?;
        let detail = value["detail"].as_str().unwrap_or_default();
        match value["event"].as_str() {
            Some("http.segmented_started") => workers = field(detail, "workers"),
            Some("http.segmented_metrics") => metrics = Some(detail.to_owned()),
            _ => {}
        }
    }
    let detail = metrics?;
    let (header, body, storage) = (
        field(&detail, "header_wait_us_sum")?,
        field(&detail, "body_wait_us_sum")?,
        field(&detail, "storage_wait_us_sum")?,
    );
    let sum = (header + body + storage).max(1.0);
    Some(EngineMetrics {
        header_pct: header / sum * 100.0,
        body_pct: body / sum * 100.0,
        storage_pct: storage / sum * 100.0,
        ranges: field(&detail, "ranges")? as u64,
        workers: workers? as u64,
    })
}

fn verdict(
    report: &mut Report,
    network: &[(usize, Series, ProbeStats)],
    disk: Option<f64>,
    engine: &Series,
    metrics: Option<&EngineMetrics>,
    total: u64,
) {
    let best = network
        .iter()
        .max_by(|a, b| a.1.steady_avg().total_cmp(&b.1.steady_avg()));
    let engine_steady = engine.steady_avg();
    if let Some((connections, series, _)) = best {
        let ceiling = series.steady_avg();
        report.say(format!(
            "• Teto da rede medido sem disco: {ceiling:.1} MB/s (melhor com {connections} conexões). O motor, depois da rampa, fez {engine_steady:.1} MB/s ({:.0}% desse teto).",
            engine_steady / ceiling.max(0.001) * 100.0
        ));
        if engine_steady >= ceiling * 0.95 {
            report.say("• O motor já usa praticamente tudo que a rede entrega. O limite está no link ou no servidor, não no motor.");
        } else {
            report.say(format!(
                "• Há perda no motor: ele fica {:.1} MB/s abaixo do que a rede entregou ao mesmo tempo. Veja a divisão do tempo por conexão acima (cabeçalhos / corpo / gravação).",
                ceiling - engine_steady
            ));
        }
    }
    let by = |n: usize| {
        network
            .iter()
            .find(|(c, ..)| *c == n)
            .map(|(_, s, _)| s.steady_avg())
    };
    if let (Some(one), Some(best)) = (by(1), best.map(|b| b.1.steady_avg())) {
        if one >= best * 0.85 {
            report.say(format!("• Uma conexão sozinha já faz {one:.1} MB/s (≥85% do teto): o servidor não limita por conexão."));
        } else {
            report.say(format!("• Uma conexão sozinha faz só {one:.1} MB/s: o servidor limita por conexão, então várias conexões são necessárias."));
        }
    }
    if let (Some(eight), Some(sixteen)) = (by(8), by(16)) {
        if sixteen < eight * 0.97 {
            report.say(format!("• 16 conexões ({sixteen:.1} MB/s) rendem MENOS que 8 ({eight:.1}): neste link/servidor mais conexões pioram; 8 seria melhor."));
        } else if sixteen < eight * 1.03 {
            report.say(format!("• 8 conexões ({eight:.1} MB/s) e 16 ({sixteen:.1}) rendem o mesmo: 16 não ajuda aqui, 8 bastam."));
        } else {
            report.say(format!("• 16 conexões ({sixteen:.1} MB/s) rendem mais que 8 ({eight:.1}): 16 ajuda neste link."));
        }
    }
    if let (Some(sixteen), Some(thirty_two)) = (by(16), by(32))
        && thirty_two > sixteen * 1.05
    {
        report.say(format!("• 32 conexões entregam {thirty_two:.1} MB/s contra {sixteen:.1} com 16: aumentar o máximo de conexões do motor (hoje 16) ajudaria."));
    }
    if let Some(disk) = disk {
        if disk < engine_steady * 1.3 {
            report.say(format!("• O disco sustentou {disk:.0} MB/s, perto da velocidade do download: o disco (ou o antivírus escaneando a gravação) pode estar limitando."));
        } else {
            report.say(format!(
                "• O disco sustentou {disk:.0} MB/s, bem acima do download: não é o gargalo."
            ));
        }
    }
    if let Some(metrics) = metrics {
        if metrics.header_pct > 3.0 {
            report.say(format!("• {:.1}% do tempo das conexões vai em esperar cabeçalho de nova requisição: faixas maiores ou requisições adiantadas reduziriam isso.", metrics.header_pct));
        }
        if metrics.storage_pct > 3.0 {
            report.say(format!("• {:.1}% do tempo das conexões vai em esperar gravação em disco: gravar em segundo plano ajudaria.", metrics.storage_pct));
        }
    }
    let dips = engine.dips();
    if let Some(ramp) = engine.ramp_seconds() {
        report.say(format!("• O motor leva {ramp} s para chegar a 90% da velocidade estável ({:.2} GB no total: a rampa pesa mais em arquivos pequenos).", total as f64 / 1e9));
    }
    if dips.is_empty() {
        report.say("• Depois da rampa o motor não caiu abaixo de 90% da sua mediana: a velocidade foi estável.");
    } else {
        let listed = dips
            .iter()
            .take(8)
            .map(|(second, speed)| format!("{second}s={speed:.0}"))
            .collect::<Vec<_>>()
            .join(", ");
        report.say(format!("• {} segundos ficaram abaixo de 90% da mediana ({listed}). Compare com a rede sem disco: se ela oscila nos mesmos tipos de intervalo, a causa é externa.", dips.len()));
    }
}
