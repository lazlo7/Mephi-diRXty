package com.requef.dirxty.disposable;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

// Combines many Disposables into one.
public class CompositeDisposable implements Disposable {
    private final Set<Disposable> disposables = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    public boolean add(Disposable disposable) {
        // If already disposed, dispose the new disposable immediately and return false.
        if (disposed.get()) {
            disposable.dispose();
            return false;
        }

        disposables.add(disposable);

        // We may have been disposed of after adding, recheck.
        if (disposed.get()) {
            if (disposables.remove(disposable)) {
                disposable.dispose();
            }
            return false;
        }
        return true;
    }

    @Override
    public void dispose() {
        if (disposed.compareAndSet(false, true)) {
            for (var disposable : disposables) {
                disposable.dispose();
            }
            disposables.clear();
        }
    }

    @Override
    public boolean isDisposed() {
        return disposed.get();
    }
}
