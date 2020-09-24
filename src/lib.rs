mod runtime;

pub mod consumer;
pub mod producer;

// Rust Bindings
pub use consumer::KafkaConsumer;
#[cfg(not(feature = "python"))]
pub use producer::produce;


// Python bindings
#[cfg(all(feature = "python"))]
use pyo3::prelude::*;
#[cfg(all(feature = "python"))]
use pyo3::{wrap_pyfunction, Python};
#[cfg(all(feature = "python"))]
use pyo3::exceptions::PyOSError;

/// produce function for Python
#[cfg(all(feature = "python"))]
#[pyfunction]
pub fn produce(topic: &str, message: &str) -> PyResult<()> {
    let res = producer::produce(topic, message);
    match res {
        Ok(_) => Ok(()),
        Err(err) => Err(PyOSError::new_err(err)),
    }
}

/// Consumer class for Python
#[cfg(all(feature = "python"))]
#[pyclass]
struct Consumer {
   c: consumer::KafkaConsumer,
}

#[cfg(all(feature = "python"))]
#[pymethods]
impl Consumer {
    #[new]
    fn new(topic_name: &str) -> PyResult<Self> {
        let maybe_consumer = KafkaConsumer::new(topic_name);
        match maybe_consumer {
            Ok(c) => Ok(Consumer{c: c}),
            Err(err) => Err(PyOSError::new_err(err)),
        }
    }

    fn consume(&mut self) -> PyResult<String> {
        let res = self.c.consume();
        match res {
            Ok(result) => Ok(result),
            Err(err) => Err(PyOSError::new_err(err)),
        }
    }
}

#[cfg(all(feature = "python"))]
#[pymodule]
fn kafka(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(produce, m)?)?;
    m.add_class::<Consumer>()?;
    Ok(())
}
