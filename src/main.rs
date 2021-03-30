use core::cell::{RefCell, UnsafeCell};
use core::fmt::Debug;
use core::future::Future;
use core::mem;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use futures::executor::block_on;
use heapless::{consts, spsc::{Consumer, Producer, Queue}, Vec, ArrayLength};

struct ActorMessage<A: Actor> {
    signal: RefCell<*const SignalSlot>,
    inner: RefCell<*mut A::Message>,
}

impl<A: Actor> ActorMessage<A> {
    pub fn new(message: &mut A::Message, signal: &SignalSlot) -> Self {
        Self { inner: RefCell::new(message), signal: RefCell::new(signal) }
    }
}

struct SignalSlot {
    free: AtomicBool,
    signal: Signal,
}

impl SignalSlot {
    fn acquire(&self) -> bool {
        if self.free.swap(false, Ordering::AcqRel) {
            self.signal.reset();
            true
        } else {
            false
        }
    }

    pub fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.signal.poll_wait(cx)
    }

    pub fn signal(&self) {
        self.signal.signal()
    }

    fn release(&self) {
        self.free.store(true, Ordering::Release)
    }
}

impl Default for SignalSlot {
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

use std::sync::Mutex;
use std::time::{SystemTime, Duration};
use std::marker::PhantomData;
use generic_array::GenericArray;

trait Actor: Sized {
    type Configuration;
    type Message: Debug;

    fn mount(&mut self, _: Self::Configuration);
    fn poll_message(
        &mut self,
        message: &mut Self::Message,
        cx: &mut Context<'_>,
    ) -> Poll<()>;
}


struct ActorContext<A: Actor, Q: ArrayLength<SignalSlot> + ArrayLength<ActorMessage<A>>> {
    message_producer: RefCell<Option<Producer<'static, ActorMessage<A>, Q>>>,
    message_consumer: RefCell<Option<Consumer<'static, ActorMessage<A>, Q>>>,
}

impl<A: Actor, Q: ArrayLength<SignalSlot> + ArrayLength<ActorMessage<A>>> ActorContext<A, Q> {
    pub fn new() -> Self {
        Self {
            message_producer: RefCell::new(None),
            message_consumer: RefCell::new(None),
        }
    }

    fn mount(
        &self,
        req: (
            Producer<'static, ActorMessage<A>, Q>,
            Consumer<'static, ActorMessage<A>, Q>,
        ),
    ) {
        let (reqp, reqc) = req;
        self.message_producer.borrow_mut().replace(reqp);
        self.message_consumer.borrow_mut().replace(reqc);
    }

    fn next_message(&self) -> Option<ActorMessage<A>> {
        self.message_consumer
            .borrow_mut()
            .as_mut()
            .unwrap()
            .dequeue()
    }

    fn enqueue_message(&self, message: ActorMessage<A>) {
        self.message_producer
            .borrow_mut()
            .as_mut()
            .unwrap()
            .enqueue(message);
    }
}

struct ActorRunner<A: Actor, Q: ArrayLength<SignalSlot> + ArrayLength<ActorMessage<A>>> {
    actor: RefCell<Option<A>>,
    current: RefCell<Option<ActorMessage<A>>>,

    state: AtomicU8,
    in_flight: AtomicBool,

    signals: UnsafeCell<GenericArray<SignalSlot, Q>>,
    messages: UnsafeCell<Queue<ActorMessage<A>, Q>>,

    context: ActorContext<A, Q>,
}

impl<A: Actor, Q: ArrayLength<SignalSlot> + ArrayLength<ActorMessage<A>>> ActorRunner<A, Q> {
    pub fn new(actor: A) -> Self {
        Self {
            actor: RefCell::new(Some(actor)),
            current: RefCell::new(None),
            state: AtomicU8::new(ActorState::READY.into()),
            in_flight: AtomicBool::new(false),
            signals: UnsafeCell::new(Default::default()),

            messages: UnsafeCell::new(Queue::new()),

            context: ActorContext::new(),
        }
    }

    pub fn mount(&'static self, config: A::Configuration, executor: &mut ActorExecutor) {
        executor.activate_actor(self);
        let req = unsafe { (&mut *self.messages.get()).split() };

        self.context.mount(req);

        self.actor.borrow_mut().as_mut().unwrap().mount(config);
    }

    fn is_waiting(&self) -> bool {
        self.state.load(Ordering::Acquire) == ActorState::WAITING as u8
    }

    fn decrement_ready(&self) {
        self.state.fetch_sub(1, Ordering::Acquire);
    }

    fn acquire_signal(&self) -> &SignalSlot {
        let mut signals = unsafe { &mut *self.signals.get() };
        let mut i = 0;
        while i < signals.len() {
            if signals[i].acquire() {
                return &signals[i];
            }
            i += 1;
        }
        panic!("not enough signals!");
    }
}

impl<A: Actor,Q: ArrayLength<SignalSlot> + ArrayLength<ActorMessage<A>> > ActorHandle<A> for ActorRunner<A, Q> {
    fn process_message<'s, 'm>(&'s self, message: &'m mut A::Message) -> ActorResponseFuture<'s, 'm> {
        let signal = self.acquire_signal();
        let message = ActorMessage::new(message, signal);
        self.context.enqueue_message(message);
        self.state.store(ActorState::READY.into(), Ordering::SeqCst);
        ActorResponseFuture::new(signal)
    }
}

trait ActorHandle<A: Actor> {
    fn process_message<'s, 'm>(&'s self, message: &'m mut A::Message) -> ActorResponseFuture<'s, 'm>;
}

struct Address<'a, A: Actor> {
    runner: &'a dyn ActorHandle<A>,
}

impl<A: Actor> Copy for Address<'_, A> {}

impl<A: Actor> Clone for Address<'_, A> {
    fn clone(&self) -> Self {
        Self {
            runner: self.runner,
        }
    }
}

impl<'a, A: Actor> Address<'a, A> {
    fn new(runner: &'a dyn ActorHandle<A>) -> Self {
        Self { runner }
    }

    async fn process(&self, message: &mut A::Message) {
        self.runner.process_message(message).await
    }
}

struct ActorResponseFuture<'s, 'm> {
    signal: &'s SignalSlot,
    _marker: PhantomData<&'m ()>,
}

impl<'s> ActorResponseFuture<'s, '_> {
    pub fn new(signal: &'s SignalSlot) -> Self {
        Self {
            signal,
            _marker: PhantomData,
        }
    }
}

impl Future for ActorResponseFuture<'_, '_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.signal.poll_wait(cx)
    }
}

impl<A: Actor, Q: ArrayLength<SignalSlot> + ArrayLength<ActorMessage<A>>> ActiveActor for ActorRunner<A, Q> {
    fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) >= ActorState::READY as u8
    }

    fn do_poll(&self) {
        println!("[ActiveActor] do_poll()");
        if self.current.borrow().is_none() {
            if let Some(next) = self.context.next_message() {
                self.current
                    .borrow_mut()
                    .replace(next);
                self.in_flight.store(true, Ordering::Release);
            } else {
                self.in_flight.store(false, Ordering::Release);
            }
        }

        if let Some(item) = &mut *self.current.borrow_mut() {
            let state_flag_handle = &self.state as *const _ as *const ();
            let raw_waker = RawWaker::new(state_flag_handle, &VTABLE);
            let waker = unsafe { Waker::from_raw(raw_waker) };
            let mut cx = Context::from_waker(&waker);

            let mut actor = self.actor.borrow_mut();
            let mut actor = actor.as_mut().unwrap();
            if let Poll::Ready(_) = actor.poll_message(unsafe { &mut **item.inner.get_mut() }, &mut cx) {
                unsafe { &**item.signal.borrow() }.signal()
            }
        }

        self.decrement_ready();
        println!(" and done {}", self.is_ready());
    }
}

unsafe impl Send for Supervised<'_> {}

pub struct ActorExecutor<'a> {
    actors: Vec<Supervised<'a>, consts::U16>,
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

struct Supervised<'a> {
    actor: &'a dyn ActiveActor,
}

trait ActiveActor {
    fn is_ready(&self) -> bool;
    fn do_poll(&self);
}

impl<'a> Supervised<'a> {
    fn new<A: Actor, Q: ArrayLength<SignalSlot> + ArrayLength<ActorMessage<A>>>(actor: &'a ActorRunner<A, Q>) -> Self {
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

impl<'a> ActorExecutor<'a> {
    pub(crate) fn new() -> Self {
        Self { actors: Vec::new() }
    }

    pub(crate) fn activate_actor<A: Actor, Q: ArrayLength<SignalSlot> + ArrayLength<ActorMessage<A>>>(&mut self, actor: &'a ActorRunner<A, Q>) {
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

// Completely and totally not safe.
fn staticize<A: Actor, Q: ArrayLength<SignalSlot> + ArrayLength<ActorMessage<A>>>(runner: &ActorRunner<A,Q>) -> &'static ActorRunner<A,Q> {
    unsafe {
        unsafe { core::mem::transmute::<_, &'static ActorRunner<A, Q>>(runner) }
    }
}

fn main() {
    let mut executor = ActorExecutor::new();
    let foo_runner = staticize(&ActorRunner::<_, consts::U4>::new(MyActor::new("foo")));
    let bar_runner = staticize(&ActorRunner::<_, consts::U4>::new(MyActor::new("bar")));

    let foo_addr = Address::new(foo_runner);
    let bar_addr = Address::new(bar_runner);

    foo_runner.mount(bar_addr, &mut executor);
    bar_runner.mount(foo_addr, &mut executor);

    std::thread::spawn(move || {
        executor.run_forever();
    });

    let mut foo_req = MyMessage::new(1, 2, 2);
    let mut bar_req = MyMessage::new(3, 4, 2);

    let foo_fut = foo_addr.process(&mut foo_req);
    let bar_fut = bar_addr.process(&mut bar_req);

    println!("block on foo");
    block_on(foo_fut);
    println!("complete foo");
    println!("block on bar");
    block_on(bar_fut);
    println!("complete bar");
    // Cheat and use other executor for the test
    println!("Foo result: {:?}", foo_req);
    println!("Bar result: {:?}", bar_req);
}

struct MyActor<'c> {
    name: &'static str,
    other: Option<Address<'c, MyActor<'c>>>,
}

impl<'a> MyActor<'a> {
    pub fn new(name: &'static str) -> Self {
        Self { name, other: None }
    }
}

#[derive(Debug)]
pub struct MyMessage {
    a: u8,
    b: u8,
    delay: u8,
    started_at: Option<SystemTime>,
    c: Option<u8>,
}

impl MyMessage {
    pub fn new(a: u8, b: u8, delay: u8) -> Self {
        Self {
            a,
            b,
            delay,
            started_at: None,
            c: None,
        }
    }
}

impl<'c> Actor for MyActor<'c> {
    type Message = MyMessage;
    type Configuration = Address<'c, MyActor<'c>>;

    fn mount(&mut self, config: Self::Configuration) {
        self.other.replace(config);
        println!("[{}] mounted!", self.name);
    }

    fn poll_message(
        &mut self,
        message: &mut Self::Message,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        match message.started_at {
            None => {
                println!("[{}] delaying request: {:?}", self.name, message);
                message.started_at.replace(SystemTime::now());
                let waker = cx.waker().clone();
                let delay = message.delay;
                let name = self.name;
                std::thread::spawn(move || {
                    println!("[{}] sleeping for {}", name, delay);
                    std::thread::sleep(Duration::from_secs(delay as u64));
                    println!("[{}] waking for {}", name, delay);
                    waker.wake();
                });
                Poll::Pending
            }
            Some(time) => {
                if let Ok(elapsed) = time.elapsed() {
                    println!("[{}] woken after {:?}", self.name, elapsed.as_secs());
                    if elapsed.as_secs() >= message.delay as u64 {
                        println!("[{}] completed request: {:?}", self.name, message);
                        return Poll::Ready(());
                    }
                }
                println!("[{}] still pending", self.name);
                Poll::Pending
            }
        }
    }
}

pub struct Signal {
    state: UnsafeCell<State>,
    lock: std::sync::Mutex<()>,
}

enum State {
    None,
    Waiting(Waker),
    Signaled,
}

unsafe impl Send for Signal {}

unsafe impl Sync for Signal {}

impl Signal {
    pub fn new() -> Self {
        Self {
            state: UnsafeCell::new(State::None),
            lock: std::sync::Mutex::new(()),
        }
    }

    fn critical_section<F, R>(&self, f: F) -> R
        where
            F: FnOnce() -> R,
    {
        let _guard = self.lock.lock().unwrap();
        f()
    }

    #[allow(clippy::single_match)]
    pub fn signal(&self) {
        self.critical_section(|| unsafe {
            let state = &mut *self.state.get();
            match mem::replace(state, State::Signaled) {
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

    pub fn poll_wait(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.critical_section(|| unsafe {
            let state = &mut *self.state.get();
            match state {
                State::None => {
                    *state = State::Waiting(cx.waker().clone());
                    Poll::Pending
                }
                State::Waiting(w) if w.will_wake(cx.waker()) => Poll::Pending,
                State::Waiting(_) => Poll::Pending,
                State::Signaled => match mem::replace(state, State::None) {
                    State::Signaled => Poll::Ready(()),
                    _ => Poll::Pending,
                },
            }
        })
    }

    pub fn signaled(&self) -> bool {
        self.critical_section(|| matches!(unsafe { &*self.state.get() }, State::Signaled))
    }
}

impl Default for Signal {
    fn default() -> Self {
        Self::new()
    }
}
