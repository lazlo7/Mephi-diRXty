package com.requef.dirxty.lib.observable;

import com.requef.dirxty.lib.observer.Observer;
import com.requef.dirxty.lib.disposable.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class CreateEmitter<T> implements ObservableEmitter<T> {
    private static final Logger log = LoggerFactory.getLogger(CreateEmitter.class);

    private final Observer<? super T> downstream;
    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final AtomicReference<Runnable> cancellable = new AtomicReference<>();

    CreateEmitter(Observer<? super T> downstream) {
        this.downstream = downstream;
    }

    @Override
    public void onNext(T item) {
        if (!isDisposed() && !done.get()) {
            downstream.onNext(item);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (done.compareAndSet(false, true)) {
            try {
                if (!isDisposed()) {
                    downstream.onError(t);
                }
            } finally {
                dispose();
            }
        }
    }

    @Override
    public void onComplete() {
        if (done.compareAndSet(false, true)) {
            try {
                if (!isDisposed()) {
                    downstream.onComplete();
                }
            } finally {
                dispose();
            }
        }
    }

    @Override
    public void setCancellable(Runnable cancellable) {
        if (!this.cancellable.compareAndSet(null, cancellable)) {
            throw new IllegalStateException("Cancellable is already set");
        }
        if (isDisposed()) {
            runCancellable(cancellable);
        }
    }

    @Override
    public void dispose() {
        if (disposed.compareAndSet(false, true)) {
            var action = cancellable.getAndSet(null);
            runCancellable(action);
        }
    }

    @Override
    public boolean isDisposed() {
        if (disposed.get()) {
            return true;
        }
        return downstream instanceof Disposable disposable && disposable.isDisposed();
    }

    private void runCancellable(Runnable action) {
        if (action == null) {
            return;
        }
        try {
            action.run();
        } catch (Throwable t) {
            log.warn("Cancellable failed", t);
        }
    }
}
