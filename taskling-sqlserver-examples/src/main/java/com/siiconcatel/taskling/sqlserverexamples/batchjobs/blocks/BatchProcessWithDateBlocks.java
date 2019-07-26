package com.siiconcatel.taskling.sqlserverexamples.batchjobs.blocks;

import com.siiconcatel.taskling.core.TasklingClient;
import com.siiconcatel.taskling.core.blocks.common.LastBlockOrder;
import com.siiconcatel.taskling.core.blocks.rangeblocks.DateRangeBlock;
import com.siiconcatel.taskling.core.blocks.rangeblocks.DateRangeBlockResponse;
import com.siiconcatel.taskling.core.contexts.CriticalSectionContext;
import com.siiconcatel.taskling.core.contexts.DateRangeBlockContext;
import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.sqlserverexamples.businesslogic.OrderRepository;
import com.siiconcatel.taskling.sqlserverexamples.model.Order;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

public class BatchProcessWithDateBlocks {
    private TasklingClient tasklingClient;
    private OrderRepository orderRepository;

    public BatchProcessWithDateBlocks(TasklingClient tasklingClient,
                              OrderRepository orderRepository) {
        this.tasklingClient = tasklingClient;
        this.orderRepository = orderRepository;
    }

    public void processC() {
        try (TaskExecutionContext executionContext = tasklingClient.createTaskExecutionContext("OrdersProcessor", "ProcessC")) {
            System.out.println("WILL TRY START TASK");
            if(executionContext.tryStart()) {
                System.out.println("STARTED TASK");

                DateRangeBlockResponse response = getDateBlocks(executionContext);

                for(DateRangeBlockContext blockContext : response.getBlockContexts()) {
                    blockContext.start();
                    System.out.println("STARTED BLOCK");

                    List<Order> orders = orderRepository.getOrdersByOrderDate(
                            toLocalDate(blockContext.getDateRangeBlock().getStartDate()),
                            toLocalDate(blockContext.getDateRangeBlock().getEndDate()));

                    for(Order order : orders) {
                        System.out.println(MessageFormat.format("OrderId: {0,number,#}, CustomerId: {1,number,#}, OrderDate: {2}, Total: {3,number,#.##}",
                                order.getOrderId(),
                                order.getCustomerId(),
                                order.getOrderDate(),
                                order.getTotalOrderValue()));
                    }

                    blockContext.complete();
                    System.out.println("COMPLETED BLOCK");
                }
            }
            else {
                System.out.println("COULD NOT START TASK");
            }
        }
    }

    public void processD() {
        try (TaskExecutionContext executionContext = tasklingClient.createTaskExecutionContext("OrdersProcessor", "ProcessD")) {
            System.out.println("WILL TRY START TASK");
            if(executionContext.tryStart()) {
                System.out.println("STARTED TASK");

                DateRangeBlockResponse response = getDateBlocksSafe(executionContext);
                for(DateRangeBlockContext blockContext : response.getBlockContexts()) {

                    blockContext.start();
                    System.out.println("STARTED BLOCK");

                    List<Order> orders = orderRepository.getOrdersByOrderDate(
                            toLocalDate(blockContext.getDateRangeBlock().getStartDate()),
                            toLocalDate(blockContext.getDateRangeBlock().getEndDate()));

                    for(Order order : orders) {
                        System.out.println(MessageFormat.format("OrderId: {0,number,#}, CustomerId: {1,number,#}, OrderDate: {2}, Total: {3,number,#.##}",
                                order.getOrderId(),
                                order.getCustomerId(),
                                order.getOrderDate(),
                                order.getTotalOrderValue()));
                        waitFor(100);
                    }

                    blockContext.complete();
                    System.out.println("COMPLETED BLOCK");
                }
            }
            else {
                System.out.println("COULD NOT START TASK");
            }
        }
    }

    private Instant getLastEndDate(TaskExecutionContext executionContext) {
        System.out.println("GETTING LAST END DATE");
        DateRangeBlock block = executionContext.getLastDateRangeBlock(LastBlockOrder.LastCreated);
        if(block == null)
            return Instant.now().minus(Duration.ofDays(365));
        else
            return block.getEndDate();
    }

    private DateRangeBlockResponse getDateBlocks(TaskExecutionContext executionContext) {
        Instant lastEndDate = getLastEndDate(executionContext);
        System.out.println("LAST END DATE IS: " + lastEndDate);
        return executionContext.getDateRangeBlocks(x -> x.withRange(lastEndDate, Instant.now(), Duration.ofDays(7)));
    }


    private DateRangeBlockResponse getDateBlocksSafe(TaskExecutionContext executionContext) {
        CriticalSectionContext cst = executionContext.createCriticalSection();

        if (cst.tryStart(Duration.ofSeconds(2), 30)) {
            try {
                Instant lastEndDate = getLastEndDate(executionContext);
                System.out.println("LAST END DATE IS: " + lastEndDate);
                return executionContext.getDateRangeBlocks(x -> x.withRange(lastEndDate, Instant.now(), Duration.ofDays(7)));
            }
            finally {
                cst.complete();
            }
        } else {
            System.out.println("COULD NOT ENTER CRITICAL SECTION");
            throw new RuntimeException("Could not safely generate blocks");
        }
    }

    private LocalDate toLocalDate(Instant instant) {
        return instant.atZone(ZoneId.of("UTC")).toLocalDate();
    }

    private void waitFor(int ms) {
        try {
            Thread.sleep(ms);
        }catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
