use core::cell::{RefCell, UnsafeCell};
use core::fmt::Debug;
use core::future::Future;
use core::mem;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use futures::executor::block_on;
use heapless::{
    consts,
    spsc::{Consumer, Producer, Queue},
    Vec,
};

struct ActorMessage<A: Actor + 'static> {
    signal: &'static SignalSlot,
    request: UnsafeCell<* mut A::Request>,
}

impl<A: Actor> ActorMessage<A> {
    pub fn new(request: &mut A::Request, signal: &'static SignalSlot) -> Self {
        Self { request: UnsafeCell::new(request), signal }
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

trait Actor: Sized {
    type Configuration;
    type Request: Debug;

    fn mount(&mut self, _: Self::Configuration);
    fn poll_request(
        &mut self,
        request: &mut Self::Request,
        cx: &mut Context<'_>,
    ) -> Poll<()>;
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

    signals: UnsafeCell<[SignalSlot; 2]>,
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

    fn acquire_signal(&'static self) -> &'static SignalSlot {
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

    fn do_request(&'static self, request: &mut A::Request) -> ActorResponseFuture {
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

    async fn request(&self, request: &mut A::Request) {
        self.runner.do_request(request).await
    }
}

struct ActorResponseFuture {
    signal: &'static SignalSlot,
}

impl ActorResponseFuture {
    pub fn new(signal: &'static SignalSlot) -> Self {
        Self { signal }
    }
}

impl Future for ActorResponseFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.signal.poll_wait(cx)
    }
}

impl<A: Actor> ActiveActor for ActorRunner<A> {
    fn is_ready(&self) -> bool {
        self.state.load(Ordering::Acquire) >= ActorState::READY as u8
    }

    fn do_poll(&'static self) {
        println!("Running self");
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

                println!("Polling future");
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
            .poll_request(unsafe { &mut **self.message.request.get() }, cx)
        {
            Poll::Ready(()) => {
                self.message.signal.signal();
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

fn main() {
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

    std::thread::spawn(move || {
        executor.run_forever();
    });

    let mut foo_req = MyRequest::new(1, 2);
    let mut bar_req = MyRequest::new(3, 4);

    block_on(foo_addr.request(&mut foo_req));
    block_on(bar_addr.request(&mut bar_req));
    // Cheat and use other executor for the test
    println!("Foo result: {:?}", foo_req);
    println!("Bar result: {:?}", bar_req);
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

#[derive(Debug)]
pub struct MyRequest {
    pub a: u8,
    pub b: u8,
    pub c: Option<u8>,
}

impl MyRequest {
    pub fn new(a: u8, b: u8) -> Self {
        Self {
            a,
            b,
            c: None,
        }
    }
}

impl Actor for MyActor {
    type Request = MyRequest;
    type Configuration = Address<MyActor>;

    fn mount(&mut self, config: Self::Configuration) {
        self.other.replace(config);
        println!("[{}] mounted!", self.name);
    }

    fn poll_request(
        &mut self,
        request: &mut Self::Request,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        println!("[{}] processing request: {:?}", self.name, request);
        request.c.replace( request.a + request.b );
        Poll::Ready(())
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
        let guard = self.lock.lock().unwrap();
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
