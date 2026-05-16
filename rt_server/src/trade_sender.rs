//
// Trade producer, reads from kafka and informs ProcessorCurr and
//   ProcessorNew

use chrono::NaiveDateTime;
use circular_buffer::CircularBuffer;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use rdkafka::consumer::StreamConsumer;
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::portfolio_sender::connect_with_retries_rd;
use crate::ref_deref::TryFromRef2;
use crate::trade::{BaseTrade, TradeRep};

// new and current processors.
use crate::processor_msg::ProcessorMiddleMessage;

const CB_LENGTH: usize = 10;

// circular buffer is composed of timestamp, and a total number of trades processed until
//   that timestamp.
type CB = CircularBuffer<CB_LENGTH, (NaiveDateTime, usize)>;

#[allow(dead_code)]
pub(crate) struct TradeProducer<T> {
    position_listener: StreamConsumer,
    processors: Vec<ActorRef<ProcessorMiddleMessage<String>>>,
    trade_list: Arc<TradeRep<T>>,
    processing_stat: Vec<CB>,
}

impl<T> TradeProducer<T> {
    pub fn new(
        kafka_server: String,
        pos_topic: String,
        processors: Vec<ActorRef<ProcessorMiddleMessage<String>>>,
        trade_list: Arc<TradeRep<T>>,
    ) -> Self {
        info!(
            "Constructing trade producer on {:?}. Connecting to topic.",
            pos_topic
        );
        let position_listener = connect_with_retries_rd(&kafka_server, &pos_topic);
        let nb_processors = processors.len();
        // let p1 = processors[0].get_name();
        let mut processing_queue = vec![];
        for _ in 0..nb_processors {
            processing_queue.push(CB::new());
        }

        Self {
            position_listener,
            processors,
            trade_list,
            processing_stat: processing_queue,
        }
    }

    // add a method that would compute the exponentially weighted running average for each processor in the processing_queue
    /// Compute exponentially weighted slope of (time, value) pairs.
    /// lambda controls decay speed (larger -> more weight on recent).
    /// Compute exponentially weighted slope of (time, value) pairs.
    /// lambda controls decay speed (bigger = more weight on recent).
    fn _exp_weighted_slope(buffer: &CB, lambda: f64) -> Option<f64> {
        if buffer.len() < 2 {
            return None;
        }

        // Convert time to seconds relative to the most recent point
        let (t_last, _) = buffer.back().unwrap();

        let mut times: Vec<f64> = Vec::new();
        let mut values: Vec<f64> = Vec::new();
        let mut weights: Vec<f64> = Vec::new();

        for (t, x) in buffer.iter() {
            let dt = (*t_last - *t).num_seconds() as f64;
            times.push(-dt); // recent = closer to zero
            values.push(*x as f64);
            weights.push((-lambda * dt).exp());
        }

        // weighted means
        let w_sum: f64 = weights.iter().sum();
        let t_mean = times
            .iter()
            .zip(weights.iter())
            .map(|(t, w)| t * w)
            .sum::<f64>()
            / w_sum;
        let x_mean = values
            .iter()
            .zip(weights.iter())
            .map(|(x, w)| x * w)
            .sum::<f64>()
            / w_sum;

        // weighted slope = cov_w(t, x) / var_w(t)
        let mut num = 0.0;
        let mut den = 0.0;

        for ((t, x), w) in times.iter().zip(values.iter()).zip(weights.iter()) {
            let dt = t - t_mean;
            let dx = x - x_mean;
            num += w * dt * dx;
            den += w * dt * dt;
        }

        if den == 0.0 {
            None
        } else {
            Some(num / den)
        }
    }

    // computes the moving averages of all buffers.
    fn _compute_all_mavg(&self) -> Vec<Option<f64>> {
        //
        let mut all_mavg = vec![];
        let lambda = 0.9; // TODO: WHAT IS THIS LAMBDA

        for cb in &self.processing_stat {
            all_mavg.push(Self::_exp_weighted_slope(cb, lambda));
        }

        all_mavg
    }
}

#[async_trait]
impl<T> Actor for TradeProducer<T>
where
    T: Send + Sync + Clone + BaseTrade + for<'a> Deserialize<'a> + TryFromRef2 + 'static,
{
    type Msg = ProcessorMiddleMessage<String>;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Initiating TradeProducer");
        Ok(())
    }

    async fn post_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Trade Producer waiting on messages.");

        loop {
            let Ok(trade_msg) = self.position_listener.recv().await else {
                error!("Listening to the trade topic failed. Ignoring.");
                continue;
            };

            let Ok(trade) = T::try_from_ref(&trade_msg) else {
                error!("Could not decode trade message. Ignoring.");
                continue;
            };

            let trade_id = trade.id();
            debug!("Received trade: {:?}", trade_id);

            self.trade_list.upsert_async(trade_id.clone(), trade).await;

            for processor in &self.processors {
                debug!("Sending trade to {:?}", processor.get_name());
                if let Err(e) =
                    processor.send_message(ProcessorMiddleMessage::NewTrade(trade_id.clone()))
                {
                    error!(
                        "Could not send NewTrade message to processor {:?}: {:?}",
                        processor.get_name(),
                        e
                    );
                }
            }

            // TODO: HERE WE HAVE TO HANDLE ProcessingStat
        }
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        _message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}
