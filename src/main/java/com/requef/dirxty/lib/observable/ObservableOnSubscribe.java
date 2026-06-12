package com.requef.dirxty.lib.observable;

@FunctionalInterface
public interface ObservableOnSubscribe<T> {
    void subscribe(ObservableEmitter<T> emitter) throws Exception;
}
