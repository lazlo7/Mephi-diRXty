package com.requef.dirxty.lib.signal;

public record Signal<T>(SignalType type, T item, Throwable error) {
    public static <T> Signal<T> next(T item) {
        return new Signal<>(SignalType.NEXT, item, null);
    }

    public static <T> Signal<T> error(Throwable error) {
        return new Signal<>(SignalType.ERROR, null, error);
    }

    public static <T> Signal<T> complete() {
        return new Signal<>(SignalType.COMPLETE, null, null);
    }
}
