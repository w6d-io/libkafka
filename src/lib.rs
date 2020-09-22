pub mod consumer;
pub mod producer;

pub use consumer::consume;
pub use producer::produce;

// Python bindings

#[cfg(all(feature = "python"))]
extern crate cpython;

#[cfg(all(feature = "python"))]
pub use cpython::{PyResult, Python, py_module_initializer, py_fn};

#[cfg(all(feature = "python"))]
pub fn produce_py(py: Python, topic: &str, message: &str) -> PyResult<String> {
    println!("python {} {}", topic, message);
    let res = produce(topic, message);
    match res {
        Ok (()) => Ok("ok".to_string()),
        Err(e) => Ok(e),
    }
}

#[cfg(all(feature = "python"))]
pub fn consume_py(py: Python, topic: &str) -> PyResult<String> {
    println!("python {}", topic);
    Ok(consume(topic).unwrap_or("error".to_string()))
}

#[cfg(all(feature = "python"))]
py_module_initializer!(kafka, |py, m| {
    m.add(py, "__doc__", "This module is implemented in Rust.")?;
    m.add(py, "produce", py_fn!(py, produce_py(topic: &str, message: &str)))?;
    m.add(py, "consume", py_fn!(py, consume_py(topic: &str)))?;
    Ok(())
});
