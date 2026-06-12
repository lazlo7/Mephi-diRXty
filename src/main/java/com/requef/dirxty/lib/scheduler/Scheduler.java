package com.requef.dirxty.lib.scheduler;

public interface Scheduler extends AutoCloseable {
    void execute(Runnable task);

    @Override
    default void close() {}
}
