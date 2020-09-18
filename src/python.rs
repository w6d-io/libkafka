// extern crate cpython;
//
// pub use cpython::{PyResult, Python, py_module_initializer, py_fn};
//
// // use crate::placeholder::{produce, consume};
//
// pub fn produce_py(_py: Python, topic: &str, message: &str) -> PyResult<String> {
//     println!("python {} {}", topic, message);
//     // produce(topic, message);
//     return Ok("ok".to_owned());
// }
//
// pub fn consume_py(_py: Python, topic: &str) -> PyResult<String> {
//     println!("python {}", topic);
//     // consume(topic);
//     return Ok("ok".to_owned());
// }
//
// py_module_initializer!(kafka, |py, m| {
//     m.add(py, "__doc__", "This module is implemented in Rust.")?;
//     m.add(py, "produce", py_fn!(py, produce_py(topic: &str, message: &str)))?;
//     m.add(py, "consume", py_fn!(py, consume_py(topic: &str)))?;
//     Ok(())
// });
