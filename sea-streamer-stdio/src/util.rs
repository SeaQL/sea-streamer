pub struct PanicGuard;

impl Drop for PanicGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            panic!("PanicGuard dropped while thread panicking");
        }
    }
}
