mod runtime;

pub mod error;
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
pub fn produce(broker: &str, topic_name: &str, message: &str) -> PyResult<()> {
    let res = producer::produce(broker, topic_name, message);
    match res {
        Ok(_) => Ok(()),
        Err(err) => Err(PyOSError::new_err(err.to_string())),
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
    fn new(broker: &str, topic_name: &str) -> PyResult<Self> {
        let maybe_consumer = KafkaConsumer::new(broker, topic_name);
        match maybe_consumer {
            Ok(c) => Ok(Consumer{c: c}),
            Err(err) => Err(PyOSError::new_err(err.to_string())),
        }
    }

    fn consume(&mut self) -> PyResult<String> {
        let res = self.c.consume();
        match res {
            Ok(result) => Ok(result),
            Err(err) => Err(PyOSError::new_err(err.to_string())),
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
