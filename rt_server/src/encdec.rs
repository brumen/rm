use time::Date;


pub trait EncoderDecoder {

    fn _decode_flight_date(flight_date: String) -> (String, Date);
    fn _encode_flight_date(flight: String, date: Date) -> String;
}
