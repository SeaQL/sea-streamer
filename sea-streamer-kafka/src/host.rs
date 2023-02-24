use mac_address::get_mac_address;
use std::{
    fs::File,
    io::{BufRead, BufReader},
};

lazy_static::lazy_static! {
    static ref HOST_ID: String = init();
}

const LEN: usize = 12;

fn init() -> String {
    if let Ok(host_id) = std::env::var("HOST_ID") {
        return host_id;
    }
    if let Ok(file) = File::open("/proc/self/cgroup") {
        // check whether this is a docker container
        let last = BufReader::new(file)
            .lines()
            .last()
            .expect("Empty file?")
            .expect("IO Error");
        if let Some((_, remaining)) = last.split_once("0::/docker/") {
            if remaining.is_empty() {
                panic!("Failed to get docker container ID");
            }
            let (container_id, _) = remaining.split_at(LEN);
            return container_id.to_owned();
        }
    }
    let mac = get_mac_address()
        .expect("Failed to get MAC address")
        .expect("There is no MAC address on this host");
    let mac = mac.to_string().replace(':', "");
    let (mac, _) = mac.split_at(LEN);
    mac.to_owned()
}

pub fn host_id() -> &'static str {
    &HOST_ID
}
