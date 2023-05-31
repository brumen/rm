use time::Date;
use time::error::{Parse, Format};  // parse error

// TODO: THIS WHOLE FILE CAN BE REMOVED IN THE NEXT ITERATION.

pub enum DecoderError {
    Parse(Parse),
    SplitError(String),
}

impl From<Parse> for DecoderError {
    fn from(p: Parse) -> DecoderError {
        DecoderError::Parse(p)
    }
}


pub trait EncoderDecoder {

    fn _decode_flight_date(flight_date: String) -> Result<(String, Date), DecoderError>;
    fn _encode_flight_date(flight: String, date: Date) -> Result<String, Format>;
}
