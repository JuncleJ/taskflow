package dev.taskflow.task;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Кооперативный сигнал: обработчик обязан завершить свой Future после остановки и очистки ресурсов.
 */
public final class CancellationToken {
    private final AtomicBoolean requested = new AtomicBoolean();

    boolean request() {
        return requested.compareAndSet(false, true);
    }

    public boolean isRequested() {
        return requested.get();
    }

    public void check() {
        if (isRequested()) throw new CancellationException("Задача отменена");
    }
}
