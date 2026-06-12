package com.requef.dirxty.observer;

import com.requef.dirxty.disposable.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

public class SafeObserver<T> implements Observer<T>, Disposable {
    private static final Logger log = LoggerFactory.getLogger(SafeObserver.class);

    private final Observer<? super T> downstream;
    private final AtomicReference<Disposable> upstream = new AtomicReference<>();

    private boolean done;
    private boolean disposed;

    public SafeObserver(Observer<? super T> downstream) {
        this.downstream = downstream;
    }

    public void setUpstream(Disposable upstream) {
        if (!this.upstream.compareAndSet(null, upstream)) {
            upstream.dispose();
            return;
        }

        if (isDisposed()) {
            upstream.dispose();
        }
    }

    @Override
    public void onNext(T item) {
        synchronized (this) {
            if (done || disposed || isDownstreamDisposed()) {
                return;
            }

            try {
                downstream.onNext(item);
            } catch (Throwable t) {
                failLocked(t);
            }
        }
    }

    @Override
    public void onError(Throwable t) {
        synchronized (this) {
            if (done || disposed || isDownstreamDisposed()) {
                log.info("Ignoring late error", t);
                return;
            }

            done = true;
            disposed = true;

            try {
                downstream.onError(t);
            } catch (Throwable callbackError) {
                log.error("Observer.onError failed", callbackError);
            } finally {
                disposeUpstreamLocked();
            }
        }
    }

    @Override
    public void onComplete() {
        synchronized (this) {
            if (done || disposed || isDownstreamDisposed()) {
                return;
            }

            done = true;
            disposed = true;

            try {
                downstream.onComplete();
            } catch (Throwable callbackError) {
                log.error("Observer.onComplete failed", callbackError);
            } finally {
                disposeUpstreamLocked();
            }
        }
    }

    @Override
    public void dispose() {
        synchronized (this) {
            if (disposed) {
                return;
            }

            disposed = true;
            disposeUpstreamLocked();
        }
    }

    @Override
    public synchronized boolean isDisposed() {
        return disposed || isDownstreamDisposed();
    }

    private void failLocked(Throwable t) {
        done = true;
        disposed = true;

        try {
            downstream.onError(t);
        } catch (Throwable callbackError) {
            log.error("Observer.onError failed", callbackError);
        } finally {
            disposeUpstreamLocked();
        }
    }

    private boolean isDownstreamDisposed() {
        return downstream instanceof Disposable disposable && disposable.isDisposed();
    }

    private void disposeUpstreamLocked() {
        var disposable = upstream.get();
        if (disposable != null) {
            disposable.dispose();
        }
    }
}
