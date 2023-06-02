// cargo test --test surveyor --features=test,runtime-tokio -- --nocapture
#[cfg(feature = "test")]
#[cfg_attr(feature = "runtime-tokio", tokio::test)]
#[cfg_attr(feature = "runtime-async-std", async_std::test)]
async fn surveyor() -> anyhow::Result<()> {
    use sea_streamer_file::{format::Beacons, MockBeacons, SurveyResult, Surveyor};
    use std::cmp::Ordering;

    env_logger::init();

    const TARGET: u32 = 3;
    let finder = |b: &Beacons| {
        if b.remaining_messages_bytes == 0 {
            SurveyResult::Undecided
        } else {
            match b.remaining_messages_bytes.cmp(&TARGET) {
                Ordering::Less | Ordering::Equal => SurveyResult::Left,
                Ordering::Greater => SurveyResult::Right,
            }
        }
    };

    // baseline, no beacon at all
    let beacons = MockBeacons::new(10);
    let surveyor = Surveyor::new(beacons, finder).await?;
    assert_eq!(surveyor.run().await?, (0, u32::MAX)); // no scope at all

    // what we are looking for is between 3 & 4
    let mut beacons = MockBeacons::new(10);
    for i in 1..=10 {
        add(&mut beacons, i);
    }
    let surveyor = Surveyor::new(beacons, finder).await?;
    assert_eq!(surveyor.run().await?, (3, 4));

    // 4 is left empty
    let mut beacons = MockBeacons::new(10);
    for i in 1..=10 {
        if i != 4 {
            add(&mut beacons, i);
        }
    }
    let surveyor = Surveyor::new(beacons, finder).await?;
    assert_eq!(surveyor.run().await?, (3, 5));

    // 3 & 4 is left empty
    let mut beacons = MockBeacons::new(10);
    for i in 1..=10 {
        if !matches!(i, 3 | 4) {
            add(&mut beacons, i);
        }
    }
    let surveyor = Surveyor::new(beacons, finder).await?;
    assert_eq!(surveyor.run().await?, (2, 5));

    // there is only 1, 8 & 9
    let mut beacons = MockBeacons::new(10);
    add(&mut beacons, 1);
    add(&mut beacons, 8);
    add(&mut beacons, 9);
    let surveyor = Surveyor::new(beacons, finder).await?;
    assert_eq!(surveyor.run().await?, (1, 8));

    // there is only 8 & 9
    let mut beacons = MockBeacons::new(10);
    add(&mut beacons, 8);
    add(&mut beacons, 9);
    let surveyor = Surveyor::new(beacons, finder).await?;
    assert_eq!(surveyor.run().await?, (0, 8));

    // there is only 1 beacon
    let mut beacons = MockBeacons::new(10);
    add(&mut beacons, 3);
    let surveyor = Surveyor::new(beacons, finder).await?;
    assert_eq!(surveyor.run().await?, (3, u32::MAX));

    // there is only 1 beacon
    let mut beacons = MockBeacons::new(10);
    add(&mut beacons, 8);
    let surveyor = Surveyor::new(beacons, finder).await?;
    assert_eq!(surveyor.run().await?, (0, 8));

    fn add(beacons: &mut MockBeacons, i: u32) {
        beacons.add(
            i,
            Beacons {
                remaining_messages_bytes: i,
                items: Vec::new(),
            },
        )
    }

    Ok(())
}
