pub mod resp;
pub mod types;

pub trait Protocol {
    fn handle(&mut self, message: &[u8], on_response: impl FnMut(&[u8])) -> usize;
}
