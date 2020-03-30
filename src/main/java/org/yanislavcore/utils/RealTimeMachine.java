package org.yanislavcore.utils;

import javax.annotation.Nonnull;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Time machine that return real time.
 */
public class RealTimeMachine implements TimeMachine {
    @Override
    @Nonnull
    public LocalDate todayUtc() {
        return LocalDate.now(ZoneOffset.UTC);
    }

    @Override
    public long epochMillis() {
        return System.currentTimeMillis();
    }
}
