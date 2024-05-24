use local_sync::oneshot::Sender;

use crate::commands::command::Command;

pub struct Message {
    pub partition_id: u32,
    pub command: Command,
    pub sender: Sender<Box<[u8]>>,
}

impl Message {
    pub fn new(partition_id: u32, command: Command, sender: Sender<Box<[u8]>>) -> Self {
        Self {
            partition_id,
            command,
            sender,
        }
    }
}

unsafe impl Send for Message {}
