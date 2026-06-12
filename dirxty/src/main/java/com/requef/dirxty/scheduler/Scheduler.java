package com.requef.dirxty.scheduler;

public interface Scheduler extends AutoCloseable {
    void execute(Runnable task);

    @Override
    default void close() {}
}
