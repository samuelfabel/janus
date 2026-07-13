// Kind of command operations
pub enum Command {
    //Delete operation
    DELETE { key: Vec<u8> },
    //Get operation
    GET { key: Vec<u8> },
    //Set operation
    SET { key: Vec<u8>, value: Vec<u8> },
}
