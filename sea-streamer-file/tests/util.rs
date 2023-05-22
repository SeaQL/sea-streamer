use std::fs::OpenOptions;

pub fn temp_file(name: &str) -> Result<String, std::io::Error> {
    let path = format!("/tmp/{name}");
    let _file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;

    Ok(path)
}
