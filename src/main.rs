use core::cell::{RefCell, UnsafeCell};
use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use futures::executor::block_on;
use heapless::{
    consts,
    spsc::{Consumer, Producer, Queue},
    Vec,
};

trait ActorMessage {}
/*
{
    fn into(self) -> Self;
    fn from<T>(message: Self) -> T;
}*/

use std::sync::Mutex;

trait Actor: Sized {
    type Configuration;
    type Request;
    type Response;

    fn mount(&mut self, _: Self::Configuration);
    fn poll_request(
        &mut self,
        request: &Self::Request,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Response>;
}

struct ActorContext<A: Actor> {
    request_producer: RefCell<Option<Producer<'static, A::Request, consts::U2>>>,
    request_consumer: RefCell<Option<Consumer<'static, A::Request, consts::U2>>>,

    response_producer: RefCell<Option<Producer<'static, A::Response, consts::U2>>>,
    response_consumer: RefCell<Option<Consumer<'static, A::Response, consts::U2>>>,
    waker: Mutex<Option<Waker>>,
}

impl<A: Actor> ActorContext<A> {
    pub fn new() -> Self {
        Self {
            request_producer: RefCell::new(None),
            request_consumer: RefCell::new(None),

            response_producer: RefCell::new(None),
            response_consumer: RefCell::new(None),

            waker: Mutex::new(None),
        }
    }

    fn mount(
        &self,
        req: (
            Producer<'static, A::Request, consts::U2>,
            Consumer<'static, A::Request, consts::U2>,
        ),
        res: (
            Producer<'static, A::Response, consts::U2>,
            Consumer<'static, A::Response, consts::U2>,
        ),
    ) {
        let (reqp, reqc) = req;
        let (resp, resc) = res;
        self.request_producer.borrow_mut().replace(reqp);
        self.request_consumer.borrow_mut().replace(reqc);

        self.response_producer.borrow_mut().replace(resp);
        self.response_consumer.borrow_mut().replace(resc);
    }

    fn next_request(&self) -> Option<A::Request> {
        self.request_consumer
            .borrow_mut()
            .as_mut()
            .unwrap()
            .dequeue()
    }

    fn enqueue_request(&self, request: A::Request) {
        self.request_producer
            .borrow_mut()
            .as_mut()
            .unwrap()
            .enqueue(request);
    }

    fn enqueue_response(&self, request: A::Response) {
        self.response_producer
            .borrow_mut()
            .as_mut()
            .unwrap()
            .enqueue(request);
        self.notify_waker();
    }

    fn notify_waker(&self) {
        let mut w = self.waker.lock().unwrap();
        if let Some(w) = w.take() {
            println!("Notifying waker");
            w.wake();
        }
    }

    fn set_waker(&self, waker: Waker) {
        print!("Setting waker");
        let mut w = self.waker.lock().unwrap();
        w.replace(waker);
    }

    fn poll_response(&self, cx: &mut Context<'_>) -> Poll<A::Response> {
        if let Some(response) = self
            .response_consumer
            .borrow_mut()
            .as_mut()
            .unwrap()
            .dequeue()
        {
            println!("Poll response yay");
            Poll::Ready(response)
        } else {
            println!("Poll response wait");
            self.set_waker(cx.waker().clone());
            Poll::Pending
        }
    }
}

struct ActorRunner<A: Actor + 'static> {
    actor: RefCell<Option<A>>,
    current: RefCell<Option<ActorRequestFuture<A>>>,

    state: AtomicU8,
    in_flight: AtomicBool,

    requests: UnsafeCell<Queue<A::Request, consts::U2>>,
    responses: UnsafeCell<Queue<A::Response, consts::U2>>,

    context: ActorContext<A>,
}

impl<A: Actor> ActorRunner<A> {
    pub fn new(actor: A) -> Self {
        Self {
            actor: RefCell::new(Some(actor)),
            current: RefCell::new(None),
            state: AtomicU8::new(ActorState::READY.into()),
            in_flight: AtomicBool::new(false),

            requests: UnsafeCell::new(Queue::new()),
            responses: UnsafeCell::new(Queue::new()),

            context: ActorContext::new(),
        }
    }

    pub fn mount(&'static self, config: A::Configuration, executor: &mut ActorExecutor) {
        executor.activate_actor(self);
        let req = unsafe { (&mut *self.requests.get()).split() };
        let res = unsafe { (&mut *self.responses.get()).split() };

        self.context.mount(req, res);

        self.actor.borrow_mut().as_mut().unwrap().mount(config);
    }

    fn is_waiting(&self) -> bool {
        self.state.load(Ordering::Acquire) == ActorState::WAITING as u8
    }

    fn decrement_ready(&self) {
        self.state.fetch_sub(1, Ordering::Acquire);
    }

    fn do_request(&'static self, request: A::Request) -> ActorResponseFuture<A> {
        self.context.enqueue_request(request);
        self.state.store(ActorState::READY.into(), Ordering::SeqCst);
        ActorResponseFuture::new(&self.context)
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
    context: &'static ActorContext<A>,
}

impl<A: Actor> ActorResponseFuture<A> {
    pub fn new(context: &'static ActorContext<A>) -> Self {
        Self { context }
    }
}

impl<A: Actor> Future for ActorResponseFuture<A> {
    type Output = A::Response;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.context.poll_response(cx)
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

                let item = Pin::new(item);
                let result = item.poll(&mut cx);
                match result {
                    Poll::Ready(value) => {
                        self.context.enqueue_response(value);
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
    request: A::Request,
}

impl<A: Actor> Unpin for ActorRequestFuture<A> {}
unsafe impl Send for Supervised {}

impl<A: Actor> ActorRequestFuture<A> {
    pub fn new(runner: &'static ActorRunner<A>, request: A::Request) -> Self {
        Self { runner, request }
    }
}

impl<A: Actor> Future for ActorRequestFuture<A> {
    type Output = A::Response;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.runner
            .actor
            .borrow_mut()
            .as_mut()
            .unwrap()
            .poll_request(&self.request, cx)
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
    let bar_addr = Address::new(foo_runner);
    foo_runner.mount(bar_addr, &mut executor);
    bar_runner.mount(foo_addr, &mut executor);

    std::thread::spawn(move || {
        executor.run_forever();
    });

    // Cheat and use other executor for the test
    println!("Foo result: {}", block_on(foo_addr.request(1)));
    println!("Bar result: {}", block_on(bar_addr.request(2)));
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
        println!("[{}] mounted!", self.name);
    }

    fn poll_request(
        &mut self,
        request: &Self::Request,
        cx: &mut Context<'_>,
    ) -> Poll<Self::Response> {
        println!("[{}] processing request: {}", self.name, request);
        Poll::Ready(*request)
    }
}
