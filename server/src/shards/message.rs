use crate::commands::command::Command;

pub struct Message {
    pub partition_id: u32,
    pub command: Command,
    pub descriptor: i32,
}

impl Message {
    pub fn new(partition_id: u32, command: Command, descriptor: i32) -> Self {
        Self {
            partition_id,
            command,
            descriptor,
        }
    }
}
