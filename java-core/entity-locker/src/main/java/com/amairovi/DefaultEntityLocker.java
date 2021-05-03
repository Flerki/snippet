package com.amairovi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DefaultEntityLocker<T> implements EntityLocker<T> {
    private final static Logger log = Logger.getLogger(DefaultEntityLocker.class.getName());

    private final ReentrancyHandler<? super T> reentrancyHandler;
    private final Map<T, Boolean> locked = new ConcurrentHashMap<>();

    public DefaultEntityLocker(ReentrancyHandler<? super T> reentrancyHandler) {
        this.reentrancyHandler = reentrancyHandler;
    }

    @Override
    public void lock(final T id) {
        // TODO replace with message supplier
        log.log(Level.FINE, "trying to lock " + id);
        while (true) {
            Boolean previous = locked.putIfAbsent(id, Boolean.TRUE);

            if (previous == null) {
                reentrancyHandler.increase(id);
                log.log(Level.FINE, "lock for " + id + " is free");
                return;
            } else {
                if (reentrancyHandler.increaseIfPresent(id)) {
                    return;
                }
                log.log(Level.FINE, "lock for " + id + " is not free");
            }
        }
    }

    @Override
    public void unlock(final T id) {
        log.log(Level.FINE, "unlocking " + id);
        Boolean isLocked = locked.get(id);
        if (isLocked == null) {
            log.log(Level.INFO, "trying to unlock not locked " + id);
            return;
        }

        if (reentrancyHandler.decrease(id) == 0) {
            locked.remove(id);
        }
    }
}
