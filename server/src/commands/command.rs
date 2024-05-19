use std::fmt::Display;

pub enum Command {
    CreatePartition(),
    SendToPartition(Vec<u8>),
    ReadFromPartition(u64),
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::CreatePartition() => write!(f, "create partition"),
            Command::SendToPartition(_) => write!(f, "send to partition"),
            Command::ReadFromPartition(_) => write!(f, "read from partition"),
        }
    }
}

impl From<u32> for Command {
    fn from(command_id: u32) -> Self {
        match command_id {
            0 => Command::CreatePartition(),
            1 => panic!("Not allowed"),
            2 => panic!("Not allowed"),
            _ => unreachable!("Invalid command id: {command_id}"),
        }
    }
}
