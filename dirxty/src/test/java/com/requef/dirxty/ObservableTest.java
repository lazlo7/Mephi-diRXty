package com.requef.dirxty;

import com.requef.dirxty.observable.Observable;
import com.requef.dirxty.observable.ObservableEmitter;
import com.requef.dirxty.observer.Observer;
import com.requef.dirxty.scheduler.Scheduler;
import com.requef.dirxty.scheduler.Schedulers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ObservableTest {
    @Test
    void createSubscribe_emitsItemsAndCompletes() {
        var observer = new TestObserver<Integer>();

        var disposable = Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        }).subscribe(observer);

        assertTrue(disposable.isDisposed());
        assertEquals(List.of(1, 2, 3), observer.values());
        assertTrue(observer.completed());
        assertNull(observer.error());
    }

    @Test
    void createSubscribe_ignoresEventsAfterComplete() {
        var observer = new TestObserver<Integer>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onComplete();
            // Should be ignored.
            emitter.onNext(2);
            emitter.onError(new IllegalStateException("late error"));
        }).subscribe(observer);

        assertEquals(List.of(1), observer.values());
        assertTrue(observer.completed());
        assertNull(observer.error());
    }

    @Test
    void createSubscribe_deliversThrownSourceExceptionToOnError() {
        var observer = new TestObserver<Integer>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            throw new IllegalStateException("source failed");
        }).subscribe(observer);

        assertEquals(List.of(1), observer.values());
        assertFalse(observer.completed());
        assertInstanceOf(IllegalStateException.class, observer.error());
        assertEquals("source failed", observer.error().getMessage());
    }

    @Test
    void map_transformsItems() {
        var observer = new TestObserver<String>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        })
        .map(i -> "item-" + i)
        .subscribe(observer);

        assertEquals(List.of("item-1", "item-2", "item-3"), observer.values());
        assertTrue(observer.completed());
        assertNull(observer.error());
    }

    @Test
    void filter_keepsOnlyMatchingItems() {
        var observer = new TestObserver<Integer>();

        Observable.<Integer>create(emitter -> {
            for (int i = 1; i <= 6; i++) {
                emitter.onNext(i);
            }
            emitter.onComplete();
        })
        .filter(i -> i % 2 == 0)
        .subscribe(observer);

        assertEquals(List.of(2, 4, 6), observer.values());
        assertTrue(observer.completed());
        assertNull(observer.error());
    }

    @Test
    void mapAndFilter_canBeChained() {
        var observer = new TestObserver<Integer>();

        Observable.<Integer>create(emitter -> {
            for (int i = 1; i <= 5; i++) {
                emitter.onNext(i);
            }
            emitter.onComplete();
        })
        .filter(i -> i % 2 == 1)
        .map(i -> i * 10)
        .subscribe(observer);

        assertEquals(List.of(10, 30, 50), observer.values());
        assertTrue(observer.completed());
        assertNull(observer.error());
    }

    @Test
    void map_mapperExceptionIsDeliveredToOnError() {
        var observer = new TestObserver<Integer>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        })
        .map(i -> {
            if (i == 2) {
                throw new IllegalStateException("boom at " + i);
            }
            return i * 10;
        })
        .subscribe(observer);

        assertEquals(List.of(10), observer.values());
        assertFalse(observer.completed());
        assertInstanceOf(IllegalStateException.class, observer.error());
        assertEquals("boom at 2", observer.error().getMessage());
    }

    @Test
    void filter_predicateExceptionIsDeliveredToOnError() {
        var observer = new TestObserver<Integer>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        })
        .filter(i -> {
            if (i == 2) {
                throw new IllegalArgumentException("bad predicate");
            }
            return true;
        })
        .subscribe(observer);

        assertEquals(List.of(1), observer.values());
        assertFalse(observer.completed());
        assertInstanceOf(IllegalArgumentException.class, observer.error());
        assertEquals("bad predicate", observer.error().getMessage());
    }

    @Test
    void flatMap_mapsItemsToInnerObservablesAndMergesResults() {
        var observer = new TestObserver<String>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onNext(3);
            emitter.onComplete();
        })
        .flatMap(i -> Observable.<String>create(inner -> {
            inner.onNext("a" + i);
            inner.onNext("b" + i);
            inner.onComplete();
        }))
        .subscribe(observer);

        assertEquals(
                List.of("a1", "b1", "a2", "b2", "a3", "b3"),
                observer.values()
        );
        assertTrue(observer.completed());
        assertNull(observer.error());
    }

    @Test
    void flatMap_completesOnlyAfterAllInnerObservablesComplete() throws InterruptedException {
        try (var computation = Schedulers.computation()) {
            var observer = new TestObserver<Integer>();

            Observable.<Integer>create(emitter -> {
                emitter.onNext(1);
                emitter.onNext(2);
                emitter.onNext(3);
                emitter.onComplete();
            })
            .flatMap(i -> Observable.<Integer>create(inner -> {
                Thread.sleep(50);
                inner.onNext(i * 10);
                inner.onComplete();
            }).subscribeOn(computation))
            .subscribe(observer);

            assertTrue(observer.awaitTerminalEvent(2, TimeUnit.SECONDS));
            assertEquals(Set.of(10, 20, 30), new HashSet<>(observer.values()));
            assertTrue(observer.completed());
            assertNull(observer.error());
        }
    }

    @Test
    void flatMap_innerErrorTerminatesWholeStream() {
        var observer = new TestObserver<Integer>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        })
        .flatMap(i -> Observable.<Integer>create(inner -> {
            if (i == 2) {
                inner.onError(new IllegalStateException("inner failed"));
                return;
            }
            inner.onNext(i * 10);
            inner.onComplete();
        }))
        .subscribe(observer);

        assertEquals(List.of(10), observer.values());
        assertFalse(observer.completed());
        assertInstanceOf(IllegalStateException.class, observer.error());
        assertEquals("inner failed", observer.error().getMessage());
    }

    @Test
    void flatMap_mapperExceptionTerminatesStream() {
        var observer = new TestObserver<Integer>();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            emitter.onNext(2);
            emitter.onComplete();
        })
        .flatMap(i -> {
            if (i == 2) {
                throw new IllegalStateException("flatMap mapper failed");
            }

            return Observable.<Integer>create(inner -> {
                inner.onNext(i * 10);
                inner.onComplete();
            });
        })
        .subscribe(observer);

        assertEquals(List.of(10), observer.values());
        assertFalse(observer.completed());
        assertInstanceOf(IllegalStateException.class, observer.error());
        assertEquals("flatMap mapper failed", observer.error().getMessage());
    }

    @Test
    void disposable_stopsFurtherEvents() {
        var observer = new TestObserver<Integer>();
        var emitterRef = new AtomicReference<ObservableEmitter<Integer>>();
        var disposable = Observable.create(emitterRef::set)
            .subscribe(observer);
        var emitter = emitterRef.get();

        emitter.onNext(1);
        disposable.dispose();

        emitter.onNext(2);
        emitter.onComplete();

        assertTrue(disposable.isDisposed());
        assertEquals(List.of(1), observer.values());
        assertFalse(observer.completed());
        assertNull(observer.error());
    }

    @Test
    void disposable_runsCancellableCallback() {
        var cancelled = new AtomicBoolean(false);
        var disposable = Observable.<Integer>create(emitter -> {
            emitter.setCancellable(() -> cancelled.set(true));
        }).subscribe(new TestObserver<>());

        assertFalse(cancelled.get());
        disposable.dispose();
        assertTrue(cancelled.get());
    }

    @Test
    void observerExceptionInOnNext_isConvertedToOnError() {
        var errorRef = new AtomicReference<Throwable>();
        var receivedItems = new AtomicInteger();

        Observable.<Integer>create(emitter -> {
            emitter.onNext(1);
            // Should be ignored because observer failed on the first item.
            emitter.onNext(2);
            emitter.onComplete();
        }).subscribe(new Observer<>() {
            @Override
            public void onNext(Integer item) {
                receivedItems.incrementAndGet();
                throw new IllegalStateException("observer failed");
            }

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
            }

            @Override
            public void onComplete() {
                fail("onComplete should not be called");
            }
        });

        assertEquals(1, receivedItems.get());
        assertInstanceOf(IllegalStateException.class, errorRef.get());
        assertEquals("observer failed", errorRef.get().getMessage());
    }

    @Test
    void subscribeOn_runsSourceOnSchedulerThread() throws InterruptedException {
        try (var io = Schedulers.io()) {
            var observer = new TestObserver<Integer>();
            var sourceThreadName = new AtomicReference<String>();

            Observable.<Integer>create(emitter -> {
                sourceThreadName.set(Thread.currentThread().getName());
                emitter.onNext(1);
                emitter.onComplete();
            })
            .subscribeOn(io)
            .subscribe(observer);

            assertTrue(observer.awaitTerminalEvent(2, TimeUnit.SECONDS));
            assertEquals(List.of(1), observer.values());
            assertTrue(observer.completed());
            assertNull(observer.error());
            assertNotNull(sourceThreadName.get());
        }
    }

    @Test
    void observeOn_runsObserverCallbacksOnSchedulerThread() throws InterruptedException {
        try (var single = Schedulers.single()) {
            var observer = new TestObserver<Integer>();
            var sourceThreadName = new AtomicReference<String>();

            Observable.<Integer>create(emitter -> {
                sourceThreadName.set(Thread.currentThread().getName());
                emitter.onNext(1);
                emitter.onNext(2);
                emitter.onComplete();
            })
            .observeOn(single)
            .subscribe(observer);

            assertTrue(observer.awaitTerminalEvent(2, TimeUnit.SECONDS));
            assertEquals(List.of(1, 2), observer.values());
            assertTrue(observer.completed());
            assertNull(observer.error());
            assertEquals(1, observer.callbackThreadNames().size());

            var observerThread = observer.callbackThreadNames().getFirst();
            assertNotEquals(
                sourceThreadName.get(),
                observerThread,
                "observeOn should move observer callbacks to another thread"
            );
        }
    }

    @Test
    void subscribeOnAndObserveOn_affectDifferentPartsOfPipeline() throws InterruptedException {
        try (var io = Schedulers.io();
             var single = Schedulers.single()) {

            var observer = new TestObserver<Integer>();
            var sourceThread = new AtomicReference<String>();
            var mapThread = new AtomicReference<String>();
            Observable.<Integer>create(emitter -> {
                    sourceThread.set(Thread.currentThread().getName());
                    emitter.onNext(10);
                    emitter.onComplete();
            })
            .subscribeOn(io)
            .map(i -> {
                mapThread.set(Thread.currentThread().getName());
                return i * 2;
            })
            .observeOn(single)
            .subscribe(observer);

            assertTrue(observer.awaitTerminalEvent(2, TimeUnit.SECONDS));
            assertEquals(List.of(20), observer.values());
            assertTrue(observer.completed());
            assertNull(observer.error());
            assertNotNull(sourceThread.get());
            assertNotNull(mapThread.get());
            assertEquals(1, observer.callbackThreadNames().size());
        }
    }

    @Test
    void singleScheduler_executesTasksSequentiallyOnOneThread() throws InterruptedException {
        try (Scheduler single = Schedulers.single()) {
            int tasks = 100;
            var done = new CountDownLatch(tasks);
            var executionOrder = new CopyOnWriteArrayList<Integer>();
            for (int i = 0; i < tasks; i++) {
                int value = i;
                single.execute(() -> {
                    executionOrder.add(value);
                    done.countDown();
                });
            }
            assertTrue(done.await(2, TimeUnit.SECONDS));
            var expectedOrder = new ArrayList<Integer>();
            for (int i = 0; i < tasks; i++) {
                expectedOrder.add(i);
            }
            assertEquals(expectedOrder, executionOrder);
        }
    }

    @Test
    void computationScheduler_executesManyTasks() throws InterruptedException {
        try (Scheduler computation = Schedulers.computation()) {
            int tasks = 200;
            var done = new CountDownLatch(tasks);
            var sum = new AtomicInteger();
            for (int i = 0; i < tasks; i++) {
                computation.execute(() -> {
                    sum.incrementAndGet();
                    done.countDown();
                });
            }
            assertTrue(done.await(2, TimeUnit.SECONDS));
            assertEquals(tasks, sum.get());
        }
    }

    @Test
    void ioScheduler_canRunTasksConcurrently() throws InterruptedException {
        try (Scheduler io = Schedulers.io()) {
            int tasks = 10;
            var allStarted = new CountDownLatch(tasks);
            var release = new CountDownLatch(1);
            var allDone = new CountDownLatch(tasks);
            for (int i = 0; i < tasks; i++) {
                io.execute(() -> {
                    allStarted.countDown();
                    try {
                        assertTrue(release.await(2, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        fail(e);
                    } finally {
                        allDone.countDown();
                    }
                });
            }
            assertTrue(allStarted.await(2, TimeUnit.SECONDS));
            release.countDown();
            assertTrue(allDone.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    void observeOn_serializesConcurrentUpstreamEmissions() throws InterruptedException {
        try (var single = Schedulers.single()) {
            int items = 500;
            var observer = new TestObserver<Integer>();
            Observable.<Integer>create(emitter -> {
                        try (var executor = Executors.newFixedThreadPool(8)) {
                            var done = new CountDownLatch(items);
                            for (int i = 0; i < items; i++) {
                                int value = i;
                                executor.execute(() -> {
                                    emitter.onNext(value);
                                    done.countDown();
                                });
                            }
                            assertTrue(done.await(2, TimeUnit.SECONDS));
                            emitter.onComplete();
                        }
                    })
                .observeOn(single)
                .subscribe(observer);

            assertTrue(observer.awaitTerminalEvent(3, TimeUnit.SECONDS));
            assertEquals(items, observer.values().size());
            assertEquals(items, new HashSet<>(observer.values()).size());
            assertTrue(observer.completed());
            assertNull(observer.error());
            assertEquals(1, observer.callbackThreadNames().size());
        }
    }

    @Test
    void flatMapWithSubscribeOnAndObserveOn_handlesMultiThreadedInnerSources() throws InterruptedException {
        try (var computation = Schedulers.computation();
             var single = Schedulers.single()) {
            var observer = new TestObserver<Integer>();
            Observable.<Integer>create(emitter -> {
                        for (int i = 1; i <= 20; i++) {
                            emitter.onNext(i);
                        }
                        emitter.onComplete();
                    })
                    .flatMap(i -> Observable.<Integer>create(inner -> {
                        inner.onNext(i * 10);
                        inner.onComplete();
                    }).subscribeOn(computation))
                .observeOn(single)
                .subscribe(observer);

            assertTrue(observer.awaitTerminalEvent(3, TimeUnit.SECONDS));

            var expected = new HashSet<>();
            for (int i = 1; i <= 20; i++) {
                expected.add(i * 10);
            }
            assertEquals(expected, new HashSet<>(observer.values()));
            assertEquals(20, observer.values().size());
            assertTrue(observer.completed());
            assertNull(observer.error());
            assertEquals(1, observer.callbackThreadNames().size());
        }
    }
}
