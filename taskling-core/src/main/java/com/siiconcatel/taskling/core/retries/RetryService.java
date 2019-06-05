package com.siiconcatel.taskling.core.retries;

import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.TransientException;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class RetryService {
    public static <T> void invokeWithRetry(Consumer<T> consumer, T request)
    {
        int interval = 5000;
        double publishExponentialBackoffExponent = 2;
        int attemptLimit = 3;
        int attemptsMade = 0;

        boolean successFullySent = false;
        Exception lastException = null;

        while (attemptsMade < attemptLimit && successFullySent == false)
        {
            try
            {
                consumer.accept(request);
                successFullySent = true;
            }
            catch (TransientException ex)
            {
                lastException = ex;
            }

            interval = (int)(interval * publishExponentialBackoffExponent);
            attemptsMade++;

            if (!successFullySent)
                waitFor(interval);
        }

        if (!successFullySent)
        {
            throw new TasklingExecutionException("A persistent transient exception has caused all retries to fail", lastException);
        }
    }

    private static void waitFor(int ms) {
        try {
            Thread.sleep(ms);
        }
        catch(InterruptedException e){}
    }
}
