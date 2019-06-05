package com.siiconcatel.taskling.core.executioncontext;

import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.SendKeepAliveRequest;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskExecutionRepository;
import com.siiconcatel.taskling.core.utils.WaitUtils;

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.time.Instant;

public class KeepAliveDaemon implements Runnable
{
    private WeakReference owner;
    private final TaskExecutionRepository taskExecutionRepository;
    private boolean completeCalled;
    private SendKeepAliveRequest sendKeepAliveRequest;
    private Duration keepAliveInterval;
    private Thread t1;

    public KeepAliveDaemon(TaskExecutionRepository taskExecutionRepository,
                           WeakReference owner,
                           SendKeepAliveRequest sendKeepAliveRequest,
                           Duration keepAliveInterval)
    {
        this.owner = owner;
        this.taskExecutionRepository = taskExecutionRepository;
        this.sendKeepAliveRequest = sendKeepAliveRequest;
        this.keepAliveInterval = keepAliveInterval;
    }

    public void stop()
    {
        completeCalled = true;

        if(t1 != null) {
            try {
                t1.join();
            }
            catch(InterruptedException e) {}
        }
    }

    public void sendKeepAlives()
    {
        t1 = new Thread(this);
        t1.start();
    }

    public void run()
    {
        Instant lastKeepAlive = Instant.now();
        taskExecutionRepository.sendKeepAlive(sendKeepAliveRequest);

        while (!completeCalled && owner.get() != null)
        {
            Duration timespanSinceLastKeepAlive = Duration.between(Instant.now(), lastKeepAlive);
            if (timespanSinceLastKeepAlive.getNano() > keepAliveInterval.getNano())
            {
                lastKeepAlive = Instant.now();
                taskExecutionRepository.sendKeepAlive(sendKeepAliveRequest);
            }

            WaitUtils.waitForMs(100);
        }
    }
}
