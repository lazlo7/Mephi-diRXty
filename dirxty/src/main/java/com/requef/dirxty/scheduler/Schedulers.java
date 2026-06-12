package com.requef.dirxty.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class Schedulers {
    private static final Logger log = LoggerFactory.getLogger(Schedulers.class);

    public static Scheduler io() {
        return new ExecutorScheduler(
            "io",
            Executors.newCachedThreadPool(namedThreadFactory("diRXty-io"))
        );
    }

    public static Scheduler computation() {
        var threads = Math.max(1, Runtime.getRuntime().availableProcessors());
        return new ExecutorScheduler(
            "computation",
            Executors.newFixedThreadPool(threads, namedThreadFactory("diRXty-computation"))
        );
    }

    public static Scheduler single() {
        return new ExecutorScheduler(
            "single",
            Executors.newSingleThreadExecutor(namedThreadFactory("diRXty-single"))
        );
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger(1);

        return task -> {
            var thread = new Thread(task, prefix + "-" + counter.getAndIncrement());
            thread.setUncaughtExceptionHandler((t, e) ->
                    log.error("Uncaught exception in {}", t.getName(), e)
            );
            return thread;
        };
    }
}
