//
// Trade producer, reads from kafka and informs ProcessorCurr and
//   ProcessorNew

use chrono::NaiveDateTime;
use circular_buffer::CircularBuffer;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
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
pub struct TradeProducer<T> {
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
        myself: ActorRef<Self::Msg>,
        _state: &mut Self::State,
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("Trade Producer waiting on first message.");
        // starting w/ the first trade.
        let trade_msg = self.position_listener.recv().await?;
        let trade_1 = T::try_from_ref(&trade_msg)?;
        let trade_1_id = trade_1.id();

        info!("First trade: {:?}", trade_1_id);
        self.trade_list.upsert_sync(trade_1_id.clone(), trade_1); // add trade to the trade list.

        myself.send_message(ProcessorMiddleMessage::NewTrade(trade_1_id))?; // first message

        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        _state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        // add trades to trade_reduce
        let trade_m = message;

        // compute the ewma of all the processors and distribute accordingly.
        // let processor_mavg = self._compute_all
        let trade_id = trade_m.get_trade().unwrap(); // TODO: MAKE SURE HERE
        debug!("Sending trade: {:?}", trade_id);
        for processor in &self.processors[..] {
            processor.send_message(
                ProcessorMiddleMessage::NewTrade(trade_id.clone()), // trade_id is a string.
            )?;
        }

        let new_msg = self.position_listener.recv().await?;
        let new_trade = T::try_from_ref(&new_msg)?;
        let new_trade_id = new_trade.id();
        // TODO: check if async is possible.
        self.trade_list
            .upsert_async(new_trade_id.clone(), new_trade)
            .await;

        myself.send_message(
            ProcessorMiddleMessage::NewTrade(new_trade_id), // new trade has id.
        )?;

        // TODO: HERE WE HAVE TO HANDLE ProcessingStat

        Ok(())
    }

    // what to do when you encounter a failure event.
    //   try to restart the actor.
    // async fn handle_supervisor_evt(
    //     &self,
    //     myself: ActorRef<Self::Msg>,
    //     event: SupervisionEvent,
    //     _state: &mut Self::State,
    // ) -> Result<(), ActorProcessingErr> {
    //     match event {
    //         SupervisionEvent::ActorFailed(child_cell, error) => {
    //             error!(
    //                 "Child {} failed: {}. Restarting...",
    //                 child_cell.get_id(),
    //                 error
    //             );

    //             // RESTART LOGIC: Spawn a new instance to replace the failed one
    //             // We pass `_myself.get_cell()` as the supervisor
    //             let (new_child, _) = Actor::spawn_linked(
    //                 myself.get_name(), // Optional Name
    //                 self,              // The Actor struct
    //                 (),                // Arguments
    //                 myself.get_cell(), // The Supervisor (this actor)
    //             )
    //             .await?;
    //         }
    //         _ => {} // Handle other events like ActorStarted or ActorStopped
    //     }
    //     Ok(())
    // }
}
