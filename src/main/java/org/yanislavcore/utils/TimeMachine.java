package org.yanislavcore.utils;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.time.LocalDate;

/**
 * Separates time operations in order to simplify time-based tests.
 */
public interface TimeMachine extends Serializable {
    /**
     * Returns current date in UTC timezone.
     * @return current date
     */
    @Nonnull
    LocalDate todayUtc();

    /**
     * Returns current epoch time in millis.
     * @return current epoch time in millis.
     */
    long epochMillis();
}
