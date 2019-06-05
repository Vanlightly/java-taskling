package com.siiconcatel.taskling.core.utils;

import java.time.Duration;
import java.util.Random;

public class WaitUtils {
    public static Random r = new Random();

    public static void waitForMs(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e){}
    }

    public static void waitForRandomMs(int maxMs) {
        try {
            Thread.sleep(r.nextInt(maxMs));
        } catch (InterruptedException e){}
    }

    public static void waitFor(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e){}
    }
}
