use std::collections::HashMap;

use log::error;
use rdkafka::message::{Header, Headers, OwnedHeaders};

use crate::{
    error::{LibKafkaError, Result},
    KafkaMessage,
};

pub use rdkafka::message::Message;

///convert kafka message headers to a hashmap
pub fn headers_to_map<T: Headers>(headers: &T) -> Result<HashMap<String, String>> {
    let size = headers.count();

    let mut map: HashMap<String, String> = HashMap::with_capacity(size);
    for header in headers.iter() {
        if let Some(val) = header.value {
            let value = match String::from_utf8(val.to_vec()){
                Ok(v) => v,
                Err(e) => {
                    error!("header value is not a valid string: {e}");
                    continue
                },
            };
            map.insert(
                header.key.to_owned(),
                value
            );
        }
    }
    Ok(map)
}

///Convert hashmap to kafka message headers.
pub fn map_to_header(map: &HashMap<String, String>) -> OwnedHeaders {
    map.iter().fold(OwnedHeaders::new(), |headers, (k, v)| {
        headers.insert(Header {
            key: k,
            value: Some(v),
        })
    })
}

///extract payload, header and key from a struct implementing the Message trait
pub fn extract_message<T: Message>(message: T) -> Result<KafkaMessage> {
    let payload = message
        .payload_view::<str>()
        .transpose()?
        .ok_or_else(|| LibKafkaError::EmptyMsgError)?;
    let key = message.key_view::<str>().transpose()?.map(str::to_string);
    let headers = match message.headers() {
        None => None,
        Some(h) => Some(headers_to_map(h)?),
    };
    Ok(KafkaMessage {
        payload: payload.to_owned(),
        headers,
        key,
    })
}

#[cfg(test)]
mod utils_test {
    use super::*;

    #[test]
    fn test_headers_to_map() {
        let headers = OwnedHeaders::new()
            .add("test1", "test1")
            .add("test2", "test2");

        let expected = HashMap::from([
            ("test1".to_owned(), "test1".to_owned()),
            ("test2".to_owned(), "test2".to_owned()),
        ]);
        let map = headers_to_map(&headers).unwrap();
        assert_eq!(map, expected);
    }

    #[test]
    fn test_map_to_header() {
        let expected = OwnedHeaders::new()
            .add("test", "test")
            .add("test2", "test2");
        let input = HashMap::from([
            ("test".to_owned(), "test".to_owned()),
            ("test2".to_owned(), "test2".to_owned()),
        ]);
        let headers = map_to_header(&input);
        let mut map = HashMap::new();
        let mut expected_map = HashMap::new();
        for i in 0..headers.count() {
            if let Some((k, v)) = headers.get_as::<str>(i) {
                map.insert(k, v.unwrap());
            }
            if let Some((k, v)) = expected.get_as::<str>(i) {
                expected_map.insert(k, v.unwrap());
            }
        }
        assert_eq!(map, expected_map);
    }
}
