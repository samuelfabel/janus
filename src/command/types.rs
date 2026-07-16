// Kind of command operations
pub enum Command {
    //Delete operation
    Delete { key: Vec<u8> },
    //Get operation
    Get { key: Vec<u8> },
    //Set operation
    Set { key: Vec<u8>, value: Vec<u8> },
}
