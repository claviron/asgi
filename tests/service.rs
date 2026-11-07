use super::*;

#[test]
fn python_yaml_names_one_application() {
    let one = "target: app:app\nmodule_path: .\nvenv: null\n";
    assert!(serde_yaml::from_str::<Config>(one).is_ok());
    let many = "applications:\n  - target: app:app\n    module_path: .\n";
    assert!(serde_yaml::from_str::<Config>(many).is_err());
}
