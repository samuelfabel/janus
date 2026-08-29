// Kind of command operations
pub enum Command<'a> {
    //Delete operation
    Delete { key: &'a [u8] },
    //Get operation
    Get { key: &'a [u8] },
    //Set operation
    Set { key: &'a [u8], value: &'a [u8] },
}
