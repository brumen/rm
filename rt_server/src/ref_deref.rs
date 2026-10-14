use rdkafka::{message::BorrowedMessage, Message};
use serde::Deserialize;

#[macro_export]
macro_rules! ref_deref_trait {
    ( $x:ty, $y:ty ) => {
        impl Deref for $x {
            type Target = $y;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl DerefMut for $x {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}

// ( $x:ty, $y:ty, $generic:tt ) => {
//     impl<$generic> Deref for $x<$generic> {
//         type Target = $y<$generic>;

//         fn deref(&self) -> &Self::Target {
//             &self.0
//         }
//     }
//     impl<$generic> DerefMut for $x<$generic> {
//         fn deref_mut(&mut self) -> &mut Self::Target {
//             &mut self.0
//         }
//     }
// };

pub trait TryFromRef<T: Sized> {
    type Error: Send + Sync + std::error::Error;

    fn try_from_ref(value: &T) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

pub trait TryFromRef2<E>
where
    for<'a> Self: Deserialize<'a>,
{
    fn try_from_ref(value: &BorrowedMessage) -> Result<Self, E>;
}

pub fn decode_from_ref<T, E>(value: &BorrowedMessage, no_payload: E) -> Result<T, E>
where
    for<'a> T: Deserialize<'a>,
    E: From<std::str::Utf8Error> + From<serde_json::Error>,
{
    let msg_value = value.payload().ok_or(no_payload)?;
    let msg_utf = std::str::from_utf8(msg_value)?;
    Ok(serde_json::from_str(msg_utf)?)
}
