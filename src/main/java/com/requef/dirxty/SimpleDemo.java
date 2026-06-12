package com.requef.dirxty;

import com.requef.dirxty.lib.observable.Observable;
import com.requef.dirxty.lib.observer.Observer;
import com.requef.dirxty.lib.scheduler.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class SimpleDemo {
    private static final Logger log = LoggerFactory.getLogger(SimpleDemo.class);

    public static void main(String[] args) throws InterruptedException {
        try (var io = Schedulers.io();
             var computation = Schedulers.computation();
             var single = Schedulers.single()
        ) {
            var normalFlowDone = new CountDownLatch(1);
            var subscription = Observable.<Integer>create(emitter -> {
                log.info("source subscribed");

                for (int i = 1; i <= 5 && !emitter.isDisposed(); i++) {
                    log.info("emit {}", i);
                    emitter.onNext(i);
                    Thread.sleep(100);
                }

                emitter.onComplete();
            }).subscribeOn(io)
            .filter(i -> {
                log.info("filter {}", i);
                    return i % 2 == 1;
            }).map(i -> {
                log.info("map {}", i);
                return i * 10;
            }).flatMap(i -> Observable.<String>create(inner -> {
                log.info("inner source for {}", i);
                inner.onNext("value=" + i);
                inner.onNext("value=" + i + ", square=" + (i * i));
                inner.onComplete();
            }).subscribeOn(computation))
            .observeOn(single)
            .subscribe(new Observer<>() {
                @Override
                public void onNext(String item) {
                    log.info("observer received: {}", item);
                }

                @Override
                public void onError(Throwable t) {
                    log.info("observer error", t);
                    normalFlowDone.countDown();
                }

                @Override
                public void onComplete() {
                    log.info("observer complete");
                    normalFlowDone.countDown();
                }
            });

            normalFlowDone.await();
            subscription.dispose();

            var errorFlowDone = new CountDownLatch(1);

            Observable.<Integer>create(emitter -> {
                emitter.onNext(1);
                emitter.onNext(2);
                emitter.onNext(3);
                emitter.onComplete();
            }).map(i -> {
                if (i == 2) {
                    throw new IllegalStateException("exception at " + i);
                }
                return i;
            }).observeOn(single)
            .subscribe(new Observer<>() {
                @Override
                public void onNext(Integer item) {
                    log.info("error-demo item: {}", item);
                }

                @Override
                public void onError(Throwable t) {
                    log.info("error-demo handled: {}", t.getMessage());
                    errorFlowDone.countDown();
                }

                @Override
                public void onComplete() {
                    log.info("error-demo complete");
                    errorFlowDone.countDown();
                }
            });

            errorFlowDone.await();
        }
    }
}