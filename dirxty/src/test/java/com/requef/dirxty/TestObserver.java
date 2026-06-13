package com.requef.dirxty;

import com.requef.dirxty.observer.Observer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TestObserver<T> implements Observer<T> {
    private final List<T> values = new CopyOnWriteArrayList<>();
    private final List<String> callbackThreadNames = new CopyOnWriteArrayList<>();
    private final AtomicReference<Throwable> error = new AtomicReference<>();
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private final CountDownLatch terminalEvent = new CountDownLatch(1);

    @Override
    public void onNext(T item) {
        callbackThreadNames.add(Thread.currentThread().getName());
        values.add(item);
    }

    @Override
    public void onError(Throwable t) {
        callbackThreadNames.add(Thread.currentThread().getName());
        error.set(t);
        terminalEvent.countDown();
    }

    @Override
    public void onComplete() {
        callbackThreadNames.add(Thread.currentThread().getName());
        completed.set(true);
        terminalEvent.countDown();
    }

    List<T> values() {
        return List.copyOf(values);
    }

    Throwable error() {
        return error.get();
    }

    boolean completed() {
        return completed.get();
    }

    List<String> callbackThreadNames() {
        return callbackThreadNames.stream()
                .distinct()
                .toList();
    }

    boolean awaitTerminalEvent(long timeout, TimeUnit unit) throws InterruptedException {
        return terminalEvent.await(timeout, unit);
    }
}
