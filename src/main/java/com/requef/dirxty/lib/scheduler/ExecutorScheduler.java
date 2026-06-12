package com.requef.dirxty.lib.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorScheduler implements Scheduler {
    private static final Logger log = LoggerFactory.getLogger(ExecutorScheduler.class);
    private final String name;
    private final ExecutorService executor;

    ExecutorScheduler(String name, ExecutorService executor) {
        this.name = name;
        this.executor = executor;
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(() -> {
            try {
                task.run();
            } catch (Throwable t) {
                log.error("Scheduler task failed: {}", name, t);
                throw t;
            }
        });
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
