use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

const CONNECTING: u8 = 0;
const WAITING: u8 = 1;
const DISCONNECTED: u8 = 2;

pub struct RunnerHealth {
    connection_state: AtomicU8,
    active_tasks: AtomicUsize,
}

impl RunnerHealth {
    pub fn new() -> Self {
        Self {
            connection_state: AtomicU8::new(CONNECTING),
            active_tasks: AtomicUsize::new(0),
        }
    }

    pub fn set_connecting(&self) {
        self.connection_state.store(CONNECTING, Ordering::Relaxed);
    }

    pub fn set_waiting(&self) {
        self.connection_state.store(WAITING, Ordering::Relaxed);
    }

    pub fn task_started(&self) {
        self.active_tasks.fetch_add(1, Ordering::Relaxed);
    }

    pub fn task_finished(&self) {
        self.active_tasks.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn set_disconnected(&self) {
        self.connection_state.store(DISCONNECTED, Ordering::Relaxed);
    }

    pub fn is_ready(&self) -> bool {
        self.connection_state.load(Ordering::Relaxed) == WAITING
    }

    pub fn as_str(&self) -> &'static str {
        match self.connection_state.load(Ordering::Relaxed) {
            CONNECTING => "connecting",
            WAITING if self.active_tasks.load(Ordering::Relaxed) > 0 => "processing",
            WAITING => "waiting_for_messages",
            DISCONNECTED => "disconnected",
            _ => "unknown",
        }
    }

    pub fn active_tasks(&self) -> usize {
        self.active_tasks.load(Ordering::Relaxed)
    }
}
