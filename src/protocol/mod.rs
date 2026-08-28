mod resp;
pub mod types;

pub trait Protocol {
    fn handle(&mut self, message: &[u8], on_response: impl for<'a> FnMut(&'a [u8])) -> usize;
}
