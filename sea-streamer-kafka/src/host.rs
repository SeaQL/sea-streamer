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
    if let Some(host_id) = std::env::var("HOST_ID").ok() {
        return host_id;
    }
    let file = File::open("/proc/self/cgroup").expect("Failed to open /proc/self/cgroup");
    let last = BufReader::new(file)
        .lines()
        .last()
        .expect("Empty file?")
        .expect("IO Error");
    if let Some((_, remaining)) = last.split_once("0::/docker/") {
        // check whether this is a docker container
        if remaining.is_empty() {
            panic!("Failed to get docker container ID");
        }
        let (mac, _) = remaining.split_at(LEN);
        mac.to_owned()
    } else {
        let mac = get_mac_address()
            .expect("Failed to get MAC address")
            .expect("There is no MAC address on this host");
        let mac = mac.to_string().replace(':', "");
        let (mac, _) = mac.split_at(LEN);
        mac.to_owned()
    }
}

pub fn host_id() -> &'static str {
    &HOST_ID
}
