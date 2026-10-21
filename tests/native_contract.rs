use std::process::Command;
#[test]
fn capabilities_expose_the_real_native_rpc_and_resume_contract() {
    let output = Command::new(env!("CARGO_BIN_EXE_nibiru-xtreme"))
        .args(["capabilities", "--json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["engine"], "nibiru-xtreme");
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["features"]["native_rpc"], true);
    assert_eq!(value["features"]["http_resume_identity"], true);
    assert_eq!(value["features"]["session_persistence"], true);
}
#[test]
fn invalid_native_serve_configuration_is_rejected_before_starting_a_service() {
    for args in [
        vec!["--listen-port=0"],
        vec!["--listen-port=9-2"],
        vec!["--seed-ratio=NaN"],
        vec!["--bt-max-peers=0"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_nibiru-xtreme"))
            .arg("serve")
            .args(args)
            .output()
            .unwrap();
        assert!(!output.status.success());
    }
}
