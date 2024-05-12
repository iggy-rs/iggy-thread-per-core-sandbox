use std::{sync::atomic::{AtomicUsize, Ordering}, task::Poll};

use futures::{task::AtomicWaker, Stream};
use sharded_queue::ShardedQueue;

pub struct Shard<'a, T> {
    pub receiver: Option<Receiver<'a , T>>,
    pub senders: Vec<Sender <'a, T>>,
    pub shard_id: usize,
}

impl<'a, T> Shard<'a, T> {
    pub fn receiver(mut self) -> Option<Receiver<'a, T>> {
        self.receiver.take()
    }
    pub fn send_to(&self, shard_id: usize, data: T) {
        let sender = self.senders.get(shard_id).unwrap();
        sender.send(data);
    }
}

#[derive(Clone)]
pub struct Receiver<'a, T> {
    channel: &'a ShardedChannel<T>,
}

pub struct Sender<'a, T> {
    channel: &'a ShardedChannel<T>,
}

impl<'a, T> Sender<'a, T> {
    pub fn send(&self, data: T) {
        self.channel.task_queue.fetch_add(1, Ordering::Relaxed);
        self.channel.queue.push_back(data);
        self.channel.waker.wake();
    }
}

pub struct ShardedChannel<T> {
    queue: ShardedQueue<T>,
    task_queue: AtomicUsize,
    waker: AtomicWaker,
}
pub trait ShardedChannelsSplit<'a, T> {
    fn unbounded(&'a self) -> (Sender<'a, T>, Receiver<'a, T>);

    fn sender(&'a self) -> Sender<'a, T>;
}

impl<'a, T> ShardedChannelsSplit<'a, T> for ShardedChannel<T> {
    fn unbounded(&'a self) -> (Sender<'a, T>, Receiver<'a, T>) {
        let tx = self.sender();
        let rx = Receiver { channel: self };

        (tx, rx)
    }

    fn sender(&'a self) -> Sender<'a, T> {
        Sender { channel: self }
    }
}

impl<'a, T> Stream for Receiver<'a, T> {
    type Item = T;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.channel.waker.register(cx.waker());

        let old = self
            .channel
            .task_queue
            .load(Ordering::Relaxed);

        if old > 0 {
            self.channel
                .task_queue
                .fetch_sub(1, Ordering::Relaxed);
            let item = self.channel.queue.pop_front_or_spin_wait_item();
            Poll::Ready(Some(item))
        } else {
            Poll::Pending
        }
    }
}