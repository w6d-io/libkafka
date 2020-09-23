mod runtime;

pub mod consumer;
pub mod producer;

// Rust Bindings
#[cfg(not(feature = "python"))]
pub use consumer::consume;
#[cfg(not(feature = "python"))]
pub use producer::produce;

// Python bindings
#[cfg(all(feature = "python"))]
use pyo3::prelude::*;
#[cfg(all(feature = "python"))]
use pyo3::{wrap_pyfunction, Python};
#[cfg(all(feature = "python"))]
use pyo3::exceptions::PyOSError;

#[cfg(all(feature = "python"))]
#[pyfunction]
pub fn produce(topic: &str, message: &str) -> PyResult<()> {
    let res = producer::produce(topic, message);
    match res {
        Ok(_) => Ok(()),
        Err(err) => Err(PyOSError::new_err(err)),
    }
}

#[cfg(all(feature = "python"))]
#[pyfunction]
pub fn consume(topic: &str) -> PyResult<String> {
    let res = consumer::consume(topic);
    match res {
        Ok(result) => Ok(result),
        Err(err) => Err(PyOSError::new_err(err)),
    }
}

#[cfg(all(feature = "python"))]
#[pymodule]
fn kafka(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(consume, m)?)?;
    m.add_function(wrap_pyfunction!(produce, m)?)?;
    Ok(())
}
