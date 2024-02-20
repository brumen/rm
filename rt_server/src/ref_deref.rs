use std::fmt::Debug;

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
    type Error: Debug + Send;

    fn try_from_ref(value: &T) -> Result<Self, Self::Error>
    where
        Self: Sized + Debug;
}
