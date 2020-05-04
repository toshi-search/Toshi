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

#[ignore]
fn da_rank() {
    let base = 190.0;
    let rank_dmg = vec![133.0, 133.0, 152.0, 152.0, 171.0, 190.0, 229.0, 304.0, 380.0, 532.0, 618.0];
    let pcts = rank_dmg.into_iter().map(|dmg| (dmg / base) * 100.0).collect::<Vec<f32>>();

    let mut c = 0;
    for x in pcts.windows(2) {
        match x {
            &[i, y] => println!("Rank: {} = {}% = {}%", c, y as u32, (y as u32 - i as u32)),
            _ => (),
        }
        c += 1;
    }

    // println!("Rank: {} = {:.1}%", i, (pct / base ) * 100.0)
}
