pub mod error;
pub mod consumer;
pub mod producer;

// Rust Bindings
pub use consumer::KafkaConsumer;
pub use producer::KafkaProducer;

// Python bindings
#[cfg(all(feature = "python"))]
use pyo3::prelude::*;
#[cfg(all(feature = "python"))]
use pyo3::{Python};
#[cfg(all(feature = "python"))]
use pyo3::exceptions::PyOSError;

/// Producer class for Python
#[cfg(all(feature = "python"))]
#[pyclass]
struct Producer {
   p: producer::KafkaProducer,
}

#[cfg(all(feature = "python"))]
#[pymethods]
impl Producer {
    #[new]
    fn new(broker: &str, topic_name: &str) -> PyResult<Self> {
        let maybe_producer = KafkaProducer::new(broker, topic_name);
        match maybe_producer {
            Ok(p) => Ok(Producer{p: p}),
            Err(err) => Err(PyOSError::new_err(err.to_string())),
        }
    }

    fn produce(&mut self, message: &str) -> PyResult<()> {
        let res = self.p.produce(message);
        match res {
            Ok(()) => Ok(()),
            Err(err) => Err(PyOSError::new_err(err.to_string())),
        }
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
    m.add_class::<Consumer>()?;
    m.add_class::<Producer>()?;
    Ok(())
}
