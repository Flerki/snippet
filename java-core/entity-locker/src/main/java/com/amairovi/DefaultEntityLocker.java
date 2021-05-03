package com.amairovi;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DefaultEntityLocker<T> implements EntityLocker<T> {
    private final static Logger log = Logger.getLogger(DefaultEntityLocker.class.getName());

    private final Map<T, Boolean> locked = new ConcurrentHashMap<>();
    private final ThreadLocal<Map<T, Integer>> reentrancyMap = ThreadLocal.withInitial(HashMap::new);

    @Override
    public void lock(final T id) {
        log.log(Level.FINE, "trying to lock " + id);
        while (true) {
            Boolean previous = locked.putIfAbsent(id, Boolean.TRUE);

            if (previous == null) {
                reentrancyMap.get().put(id, 1);
                log.log(Level.FINE, "lock for " + id + " is free");
                return;
            } else {
                Integer amountOfLocks = reentrancyMap.get().get(id);
                if (amountOfLocks != null) {
                    reentrancyMap.get().put(id, amountOfLocks + 1);
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

        Integer amountOfLocks = reentrancyMap.get().get(id);
        if (amountOfLocks == 1) {
            reentrancyMap.get().remove(id);
            locked.remove(id);
        } else {
            reentrancyMap.get().put(id, amountOfLocks - 1);
        }
    }
}
