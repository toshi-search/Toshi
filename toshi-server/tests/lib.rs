#![allow(unused)]

use std::process;
use std::process::Child;
use std::time::Duration;

pub fn run_cli() -> Child {
    let cli = env!("CARGO_BIN_EXE_toshi");
    dbg!(cli);
    std::process::Command::new(cli)
        .arg("-c")
        .arg("C:\\Users\\shcar\\IdeaProjects\\Toshi\\config\\config.toml")
        .stdin(process::Stdio::piped())
        .stdout(process::Stdio::piped())
        .stderr(process::Stdio::piped())
        .spawn()
        .expect("Did not start")
}

#[ignore]
async fn test_client() {
    let mut cli = run_cli();
    std::thread::sleep(Duration::from_millis(1000));

    cli.kill().unwrap();
    let (o, e) = cli
        .wait_with_output()
        .map(|result| {
            let output: Vec<String> = String::from_utf8_lossy(&result.stdout).lines().map(Into::into).collect();
            let errors: Vec<String> = String::from_utf8_lossy(&result.stderr).lines().map(Into::into).collect();
            (output, errors)
        })
        .unwrap();

    dbg!(o);
    dbg!(e);

    // let client = hyper::client::Client::new();
    // let client = toshi::HyperToshi::with_client("http://localhost:8080", client);
    // let index = client.index().await.unwrap();

    // dbg!(index);
}
