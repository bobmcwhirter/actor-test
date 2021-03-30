#![no_main]
#![no_std]
use core::cell::{RefCell, UnsafeCell};
use core::fmt::Debug;
use core::future::Future;
use core::mem;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use cortex_m_rt::{entry, exception};
use heapless::{
    consts,
    spsc::{Consumer, Producer, Queue},
    Vec,
};
use panic_reset as _;

use nrf52833_hal as hal;

struct ActorMessage<A: Actor + 'static> {
    signal: &'static SignalSlot<A::Response>,
    request: A::Request,
}

impl<A: Actor> ActorMessage<A> {
    pub fn new(request: A::Request, signal: &'static SignalSlot<A::Response>) -> Self {
        Self { request, signal }
    }
}

struct SignalSlot<T: Sized> {
    free: AtomicBool,
    signal: Signal<T>,
}

impl<T> SignalSlot<T> {
    fn acquire(&self) -> bool {
        if self.free.swap(false, Ordering::AcqRel) {
            self.signal.reset();
            true
        } else {
            false
        }
    }

    pub fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<T> {
        self.signal.poll_wait(cx)
    }

    pub fn signal(&self, val: T) {
        self.signal.signal(val)
    }

    fn release(&self) {
        self.free.store(true, Ordering::Release)
    }
}

impl<T: Sized> Default for SignalSlot<T> {
    fn default() -> Self {
        Self {
            free: AtomicBool::new(true),
            signal: Signal::new(),
        }
    }
}

/*
{
    fn into(self) -> Self;
    fn from<T>(message: Self) -> T;
}*/

trait Actor: Sized {
    type Configuration;
    type Request: Debug;
    type Response: Debug;

    fn mount(&mut self, _: Self::Configuration);
    fn poll_request(
        &mut self,
        request: &Self::Request,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Response>;
}

struct ActorContext<A: Actor + 'static> {
    request_producer: RefCell<Option<Producer<'static, ActorMessage<A>, consts::U2>>>,
    request_consumer: RefCell<Option<Consumer<'static, ActorMessage<A>, consts::U2>>>,
}

impl<A: Actor> ActorContext<A> {
    pub fn new() -> Self {
        Self {
            request_producer: RefCell::new(None),
            request_consumer: RefCell::new(None),
        }
    }

    fn mount(
        &self,
        req: (
            Producer<'static, ActorMessage<A>, consts::U2>,
            Consumer<'static, ActorMessage<A>, consts::U2>,
        ),
    ) {
        let (reqp, reqc) = req;
        self.request_producer.borrow_mut().replace(reqp);
        self.request_consumer.borrow_mut().replace(reqc);
    }

    fn next_request(&self) -> Option<ActorMessage<A>> {
        self.request_consumer
            .borrow_mut()
            .as_mut()
            .unwrap()
            .dequeue()
    }

    fn enqueue_request(&self, request: ActorMessage<A>) {
        self.request_producer
            .borrow_mut()
            .as_mut()
            .unwrap()
            .enqueue(request);
    }
}

struct ActorRunner<A: Actor + 'static> {
    actor: RefCell<Option<A>>,
    current: RefCell<Option<ActorRequestFuture<A>>>,

    state: AtomicU8,
    in_flight: AtomicBool,

    signals: UnsafeCell<[SignalSlot<A::Response>; 2]>,
    requests: UnsafeCell<Queue<ActorMessage<A>, consts::U2>>,

    context: ActorContext<A>,
}

impl<A: Actor> ActorRunner<A> {
    pub fn new(actor: A) -> Self {
        Self {
            actor: RefCell::new(Some(actor)),
            current: RefCell::new(None),
            state: AtomicU8::new(ActorState::READY.into()),
            in_flight: AtomicBool::new(false),
            signals: UnsafeCell::new(Default::default()),

            requests: UnsafeCell::new(Queue::new()),

            context: ActorContext::new(),
        }
    }

    pub fn mount(&'static self, config: A::Configuration, executor: &mut ActorExecutor) {
        executor.activate_actor(self);
        let req = unsafe { (&mut *self.requests.get()).split() };

        self.context.mount(req);

        self.actor.borrow_mut().as_mut().unwrap().mount(config);
    }

    fn is_waiting(&self) -> bool {
        self.state.load(Ordering::Acquire) == ActorState::WAITING as u8
    }

    fn decrement_ready(&self) {
        self.state.fetch_sub(1, Ordering::Acquire);
    }

    fn acquire_signal(&'static self) -> &'static SignalSlot<A::Response> {
        let mut signals = unsafe { (&mut *self.signals.get()) };
        let mut i = 0;
        while i < signals.len() {
            if signals[i].acquire() {
                return &signals[i];
            }
            i += 1;
        }
        panic!("not enough signals!");
    }

    fn do_request(&'static self, request: A::Request) -> ActorResponseFuture<A> {
        let signal = self.acquire_signal();
        let message = ActorMessage::new(request, signal);
        self.context.enqueue_request(message);
        self.state.store(ActorState::READY.into(), Ordering::SeqCst);
        ActorResponseFuture::new(signal)
    }
}

struct Address<A: Actor + 'static> {
    runner: &'static ActorRunner<A>,
}

impl<A: Actor> Copy for Address<A> {}

impl<A: Actor> Clone for Address<A> {
    fn clone(&self) -> Self {
        Self {
            runner: self.runner,
        }
    }
}

impl<A: Actor> Address<A> {
    fn new(runner: &'static ActorRunner<A>) -> Self {
        Self { runner }
    }

    async fn request(&self, request: A::Request) -> A::Response {
        self.runner.do_request(request).await
    }
}

struct ActorResponseFuture<A: Actor + 'static> {
    signal: &'static SignalSlot<A::Response>,
}

impl<A: Actor> ActorResponseFuture<A> {
    pub fn new(signal: &'static SignalSlot<A::Response>) -> Self {
        Self { signal }
    }
}

impl<A: Actor> Future for ActorResponseFuture<A> {
    type Output = A::Response;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.signal.poll_wait(cx)
    }
}

impl<A: Actor> ActiveActor for ActorRunner<A> {
    fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) >= ActorState::READY as u8
    }

    fn do_poll(&'static self) {
        log::info!("Running self");
        loop {
            if self.current.borrow().is_none() {
                if let Some(next) = self.context.next_request() {
                    self.current
                        .borrow_mut()
                        .replace(ActorRequestFuture::new(self, next));
                    self.in_flight.store(true, Ordering::Release);
                } else {
                    self.in_flight.store(false, Ordering::Release);
                }
            }

            let should_drop;
            if let Some(item) = &mut *self.current.borrow_mut() {
                let state_flag_handle = &self.state as *const _ as *const ();
                let raw_waker = RawWaker::new(state_flag_handle, &VTABLE);
                let waker = unsafe { Waker::from_raw(raw_waker) };
                let mut cx = Context::from_waker(&waker);

                log::info!("Polling future");
                let item = Pin::new(item);
                let result = item.poll(&mut cx);
                match result {
                    Poll::Ready(_) => {
                        should_drop = true;
                    }
                    Poll::Pending => {
                        break;
                    }
                }
            } else {
                break;
            }

            if should_drop {
                let _ = self.current.borrow_mut().take().unwrap();
            }
        }
        self.decrement_ready();
    }
}

struct ActorRequestFuture<A: Actor + 'static> {
    runner: &'static ActorRunner<A>,
    message: ActorMessage<A>,
}

impl<A: Actor> Unpin for ActorRequestFuture<A> {}
unsafe impl Send for Supervised {}

impl<A: Actor> ActorRequestFuture<A> {
    pub fn new(runner: &'static ActorRunner<A>, message: ActorMessage<A>) -> Self {
        Self { runner, message }
    }
}

impl<A: Actor> Future for ActorRequestFuture<A> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self
            .runner
            .actor
            .borrow_mut()
            .as_mut()
            .unwrap()
            .poll_request(&self.message.request, cx)
        {
            Poll::Ready(value) => {
                self.message.signal.signal(value);
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct ActorExecutor {
    actors: Vec<Supervised, consts::U16>,
}

#[derive(PartialEq)]
pub(crate) enum ActorState {
    WAITING = 0,
    READY = 1,
}

impl Into<u8> for ActorState {
    fn into(self) -> u8 {
        self as u8
    }
}

struct Supervised {
    actor: &'static dyn ActiveActor,
}

trait ActiveActor {
    fn is_ready(&self) -> bool;
    fn do_poll(&'static self);
}

impl Supervised {
    fn new<A: Actor>(actor: &'static ActorRunner<A>) -> Self {
        Self { actor }
    }

    fn poll(&mut self) -> bool {
        if self.actor.is_ready() {
            self.actor.do_poll();
            true
        } else {
            false
        }
    }
}

impl ActorExecutor {
    pub(crate) fn new() -> Self {
        Self { actors: Vec::new() }
    }

    pub(crate) fn activate_actor<A: Actor>(&mut self, actor: &'static ActorRunner<A>) {
        let supervised = Supervised::new(actor);
        self.actors
            .push(supervised)
            .unwrap_or_else(|_| panic!("too many actors"));
    }

    pub(crate) fn run_until_quiescence(&mut self) {
        let mut run_again = true;
        while run_again {
            run_again = false;
            for actor in self.actors.iter_mut() {
                if actor.poll() {
                    run_again = true
                }
            }
        }
    }

    pub fn run_forever(&mut self) -> ! {
        loop {
            self.run_until_quiescence();
        }
    }
}

// NOTE `*const ()` is &AtomicU8
static VTABLE: RawWakerVTable = {
    unsafe fn clone(p: *const ()) -> RawWaker {
        RawWaker::new(p, &VTABLE)
    }
    unsafe fn wake(p: *const ()) {
        wake_by_ref(p)
    }

    unsafe fn wake_by_ref(p: *const ()) {
        (&*(p as *const AtomicU8)).fetch_add(1, Ordering::AcqRel);
    }

    unsafe fn drop(_: *const ()) {}

    RawWakerVTable::new(clone, wake, wake_by_ref, drop)
};

#[entry]
fn main() -> ! {
    let mut executor = ActorExecutor::new();
    let foo_runner = ActorRunner::new(MyActor::new("foo"));
    let bar_runner = ActorRunner::new(MyActor::new("bar"));

    let foo_runner =
        unsafe { core::mem::transmute::<_, &'static ActorRunner<MyActor>>(&foo_runner) };
    let bar_runner =
        unsafe { core::mem::transmute::<_, &'static ActorRunner<MyActor>>(&bar_runner) };

    let foo_addr = Address::new(foo_runner);
    let bar_addr = Address::new(bar_runner);
    foo_runner.mount(bar_addr, &mut executor);
    bar_runner.mount(foo_addr, &mut executor);

    // Trigger running
    foo_addr.request(1);

    executor.run_forever();
}

struct MyActor {
    name: &'static str,
    other: Option<Address<MyActor>>,
}

impl MyActor {
    pub fn new(name: &'static str) -> Self {
        Self { name, other: None }
    }
}

impl Actor for MyActor {
    type Request = u8;
    type Response = u8;
    type Configuration = Address<MyActor>;

    fn mount(&mut self, config: Self::Configuration) {
        self.other.replace(config);
        log::info!("[{}] mounted!", self.name);
    }

    fn poll_request(
        &mut self,
        request: &Self::Request,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Response> {
        log::info!("[{}] processing request: {}", self.name, request);
        Poll::Ready(*request + 1)
    }
}

pub struct Signal<T> {
    state: UnsafeCell<State<T>>,
    locked: AtomicBool,
}

enum State<T> {
    None,
    Waiting(Waker),
    Signaled(T),
}

unsafe impl<T: Sized> Send for Signal<T> {}
unsafe impl<T: Sized> Sync for Signal<T> {}

impl<T: Sized> Signal<T> {
    pub fn new() -> Self {
        Self {
            state: UnsafeCell::new(State::None),
            locked: AtomicBool::new(false),
        }
    }

    fn critical_section<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        while self.locked.swap(true, Ordering::AcqRel) {}
        let r = f();
        self.locked.store(false, Ordering::AcqRel);
        r
    }

    #[allow(clippy::single_match)]
    pub fn signal(&self, val: T) {
        self.critical_section(|| unsafe {
            let state = &mut *self.state.get();
            match mem::replace(state, State::Signaled(val)) {
                State::Waiting(waker) => waker.wake(),
                _ => {}
            }
        })
    }

    pub fn reset(&self) {
        self.critical_section(|| unsafe {
            let state = &mut *self.state.get();
            *state = State::None
        })
    }

    pub fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<T> {
        self.critical_section(|| unsafe {
            let state = &mut *self.state.get();
            match state {
                State::None => {
                    *state = State::Waiting(cx.waker().clone());
                    Poll::Pending
                }
                State::Waiting(w) if w.will_wake(cx.waker()) => Poll::Pending,
                State::Waiting(_) => Poll::Pending,
                State::Signaled(_) => match mem::replace(state, State::None) {
                    State::Signaled(res) => Poll::Ready(res),
                    _ => Poll::Pending,
                },
            }
        })
    }

    pub fn signaled(&self) -> bool {
        self.critical_section(|| matches!(unsafe { &*self.state.get() }, State::Signaled(_)))
    }
}

impl<T: Sized> Default for Signal<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[exception]
fn DefaultHandler(irqn: i16) {}
