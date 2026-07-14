pub enum Response {
    Empty,
    Value(Option<Vec<u8>>),
    Boolean(bool),
}
