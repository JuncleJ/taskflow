package dev.taskflow.task;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Кооперативный сигнал: обработчик обязан завершить свой Future после остановки и очистки ресурсов.
 */
public final class CancellationToken {
    private final AtomicBoolean requested = new AtomicBoolean();
    private final CompletableFuture<Void> signal = new CompletableFuture<>();

    boolean request() {
        if (!requested.compareAndSet(false, true)) return false;
        signal.complete(null);
        return true;
    }

    /**
     * Callback должен быть неблокирующим; он может вызываться из потока, запросившего отмену.
     */
    public void onCancellation(Runnable handler) {
        signal.thenRun(handler);
    }

    public boolean isRequested() {
        return requested.get();
    }

    public void check() {
        if (isRequested()) throw new CancellationException("Задача отменена");
    }
}
