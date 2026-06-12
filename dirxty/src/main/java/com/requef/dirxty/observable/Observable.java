package com.requef.dirxty.observable;

import com.requef.dirxty.observer.Observer;
import com.requef.dirxty.observer.SafeObserver;
import com.requef.dirxty.scheduler.Scheduler;
import com.requef.dirxty.disposable.CompositeDisposable;
import com.requef.dirxty.disposable.Disposable;
import com.requef.dirxty.signal.Signal;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

public class Observable<T> {
    private final OnSubscribe<T> source;

    private Observable(OnSubscribe<T> source) {
        this.source = source;
    }

    public static <T> Observable<T> create(ObservableOnSubscribe<T> source) {
        return new Observable<>(observer -> {
            var emitter = new CreateEmitter<T>(observer);

            try {
                source.subscribe(emitter);
            } catch (Throwable t) {
                emitter.onError(t);
            }

            return emitter;
        });
    }

    public Disposable subscribe(Observer<? super T> observer) {
        var safeObserver = new SafeObserver<T>(observer);
        try {
            var upstream = source.subscribe(safeObserver);
            safeObserver.setUpstream(upstream);
        } catch (Throwable t) {
            safeObserver.onError(t);
        }

        return safeObserver;
    }

    public <R> Observable<R> map(Function<? super T, ? extends R> mapper) {
        return new Observable<>(observer -> Observable.this.subscribe(new Observer<>() {
            @Override
            public void onNext(T item) {
                var mapped = mapper.apply(item);
                observer.onNext(mapped);
            }

            @Override
            public void onError(Throwable t) {
                observer.onError(t);
            }

            @Override
            public void onComplete() {
                observer.onComplete();
            }
        }));
    }

    public Observable<T> filter(Predicate<? super T> predicate) {
        return new Observable<>(observer -> Observable.this.subscribe(new Observer<>() {
            @Override
            public void onNext(T item) {
                if (predicate.test(item)) {
                    observer.onNext(item);
                }
            }

            @Override
            public void onError(Throwable t) {
                observer.onError(t);
            }

            @Override
            public void onComplete() {
                observer.onComplete();
            }
        }));
    }

    public <R> Observable<R> flatMap(Function<? super T, Observable<R>> mapper) {
        return new Observable<>(observer -> {
            var disposables = new CompositeDisposable();
            var activeSources = new AtomicInteger(1);
            var terminated = new AtomicBoolean(false);
            var downstreamLock = new Object();

            var outerObserver = new Observer<T>() {
                @Override
                public void onNext(T item) {
                    if (terminated.get() || disposables.isDisposed()) {
                        return;
                    }

                    var innerObservable = mapper.apply(item);
                    activeSources.incrementAndGet();
                    var innerDisposable = innerObservable.subscribe(new Observer<>() {
                        @Override
                        public void onNext(R innerItem) {
                            if (terminated.get() || disposables.isDisposed()) {
                                return;
                            }

                            synchronized (downstreamLock) {
                                if (!terminated.get() && !disposables.isDisposed()) {
                                    observer.onNext(innerItem);
                                }
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            terminateWithError(t);
                        }

                        @Override
                        public void onComplete() {
                            completeOneSource();
                        }
                    });

                    disposables.add(innerDisposable);
                }

                @Override
                public void onError(Throwable t) {
                    terminateWithError(t);
                }

                @Override
                public void onComplete() {
                    completeOneSource();
                }

                private void terminateWithError(Throwable t) {
                    if (terminated.compareAndSet(false, true)) {
                        disposables.dispose();
                        synchronized (downstreamLock) {
                            observer.onError(t);
                        }
                    }
                }

                private void completeOneSource() {
                    if (activeSources.decrementAndGet() == 0 && terminated.compareAndSet(false, true)) {
                        disposables.dispose();
                        synchronized (downstreamLock) {
                            observer.onComplete();
                        }
                    }
                }
            };

            var outerDisposable = Observable.this.subscribe(outerObserver);
            disposables.add(outerDisposable);

            return disposables;
        });
    }

    public Observable<T> subscribeOn(Scheduler scheduler) {
        return new Observable<>(observer -> {
            var disposables = new CompositeDisposable();

            scheduler.execute(() -> {
                if (disposables.isDisposed()) {
                    return;
                }

                var upstream = Observable.this.subscribe(observer);
                disposables.add(upstream);
            });

            return disposables;
        });
    }

    public Observable<T> observeOn(Scheduler scheduler) {
        return new Observable<>(observer -> {
            var disposables = new CompositeDisposable();
            var queue = new ConcurrentLinkedQueue<Signal<T>>();
            var workInProgress = new AtomicInteger(0);

            Runnable drain = () -> {
                int missed = 1;

                while (true) {
                    if (disposables.isDisposed()) {
                        queue.clear();
                        return;
                    }

                    Signal<T> signal;
                    while ((signal = queue.poll()) != null) {
                        if (disposables.isDisposed()) {
                            queue.clear();
                            return;
                        }

                        switch (signal.type()) {
                            case NEXT -> observer.onNext(signal.item());
                            case ERROR -> {
                                disposables.dispose();
                                observer.onError(signal.error());
                                return;
                            }
                            case COMPLETE -> {
                                disposables.dispose();
                                observer.onComplete();
                                return;
                            }
                        }
                    }

                    missed = workInProgress.addAndGet(-missed);
                    if (missed == 0) {
                        return;
                    }
                }
            };

            var scheduledObserver = new Observer<T>() {
                @Override
                public void onNext(T item) {
                    enqueue(Signal.next(item));
                }

                @Override
                public void onError(Throwable t) {
                    enqueue(Signal.error(t));
                }

                @Override
                public void onComplete() {
                    enqueue(Signal.complete());
                }

                private void enqueue(Signal<T> signal) {
                    if (disposables.isDisposed()) {
                        return;
                    }

                    queue.offer(signal);
                    if (workInProgress.getAndIncrement() == 0) {
                        scheduler.execute(drain);
                    }
                }
            };

            var upstream = Observable.this.subscribe(scheduledObserver);
            disposables.add(upstream);

            return disposables;
        });
    }

    @FunctionalInterface
    private interface OnSubscribe<T> {
        Disposable subscribe(Observer<? super T> observer);
    }
}
