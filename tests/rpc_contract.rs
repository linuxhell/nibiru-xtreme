use nibiru_xtreme::engine::safe_output_name;

#[test]
fn output_name_contract_is_strict() {
    assert!(safe_output_name("ubuntu.iso"));
    assert!(!safe_output_name("../ubuntu.iso"));
    assert!(!safe_output_name("folder/ubuntu.iso"));
}
