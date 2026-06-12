package com.requef.dirxty.observable;

import com.requef.dirxty.disposable.Disposable;

public interface ObservableEmitter<T> extends Disposable {
    void onNext(T item);
    void onError(Throwable t);
    void onComplete();
    void setCancellable(Runnable cancellable);
}
