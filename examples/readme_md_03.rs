use std::{cell::Cell, rc::Rc, time::Duration};

use adaptite::event;
use runite::{main, queue_future, set_interval, time::sleep};

#[main]
fn main() {
    let my_event = event::<String>();

    my_event.subscribe(|message| {
        println!("got event with message: {message}");
    });

    // Emit an event every 250ms with an incrementing count.
    let interval = set_interval(Duration::from_millis(250), {
        let counter = Rc::new(Cell::new(0));
        move || {
            let count = counter.get();
            my_event.emit(format!("the count is {}", count));
            counter.set(count + 1);
        }
    });

    // After 5 seconds, clear the interval to stop emitting events.
    queue_future(async move {
        sleep(Duration::from_secs(5)).await;
        interval.clear();
    });
}
