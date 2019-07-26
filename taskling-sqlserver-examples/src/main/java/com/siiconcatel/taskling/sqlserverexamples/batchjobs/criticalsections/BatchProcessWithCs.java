package com.siiconcatel.taskling.sqlserverexamples.batchjobs.criticalsections;

import com.siiconcatel.taskling.core.TasklingClient;
import com.siiconcatel.taskling.core.contexts.CriticalSectionContext;
import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.sqlserverexamples.businesslogic.OrderRepository;
import com.siiconcatel.taskling.sqlserverexamples.model.Order;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.List;

public class BatchProcessWithCs {
    private TasklingClient tasklingClient;
    private OrderRepository orderRepository;

    public BatchProcessWithCs(TasklingClient tasklingClient,
                              OrderRepository orderRepository) {
        this.tasklingClient = tasklingClient;
        this.orderRepository = orderRepository;
    }

    public void processA() {
        try (TaskExecutionContext executionContext = tasklingClient.createTaskExecutionContext("OrdersProcessor", "ProcessA")) {
            System.out.println("WILL TRY START TASK");
            if(executionContext.tryStart()) {
                System.out.println("STARTED TASK");

                CriticalSectionContext cst = executionContext.createCriticalSection();
                if(cst.tryStart(Duration.ofSeconds(0), 0)) {
                    try {
                        System.out.println("START OF CRITICAL SECTION");
                        waitFor(5000);
                        System.out.println("END OF CRITICAL SECTION");
                    }
                    finally {
                        cst.complete();
                    }
                }
                else {
                    System.out.println("COULD NOT ENTER CRITICAL SECTION");
                }
            }
            else {
                System.out.println("COULD NOT START TASK");
            }
        }


    }

    public void processB() {
        try (TaskExecutionContext executionContext = tasklingClient.createTaskExecutionContext("OrdersProcessor", "ProcessB")) {
            System.out.println("WILL TRY START");
            if(executionContext.tryStart()) {
                System.out.println("STARTED");

                CriticalSectionContext cst = executionContext.createCriticalSection();
                if(cst.tryStart(Duration.ofSeconds(5), 3)) {
                    System.out.println("START OF CRITICAL SECTION");
                    waitFor(5000);
                    System.out.println("END OF CRITICAL SECTION");
                }
                else {
                    System.out.println("COULD NOT ENTER CRITICAL SECTION");
                }
            }
            else {
                System.out.println("COULD NOT START");
            }
        }
    }

    private void waitFor(int ms) {
        try {
            Thread.sleep(ms);
        }catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
