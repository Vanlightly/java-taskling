package com.siiconcatel.taskling.sqlserverexamples.batchjobs.blocks;

import com.siiconcatel.taskling.core.TasklingClient;
import com.siiconcatel.taskling.core.blocks.listblocks.*;
import com.siiconcatel.taskling.core.blocks.rangeblocks.NumericRangeBlockResponse;
import com.siiconcatel.taskling.core.contexts.CriticalSectionContext;
import com.siiconcatel.taskling.core.contexts.ListBlockContext;
import com.siiconcatel.taskling.core.contexts.NumericRangeBlockContext;
import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.sqlserverexamples.businesslogic.OrderRepository;
import com.siiconcatel.taskling.sqlserverexamples.model.Order;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BatchProcessWithListBlocks {
    private TasklingClient tasklingClient;
    private OrderRepository orderRepository;

    public BatchProcessWithListBlocks(TasklingClient tasklingClient,
                                         OrderRepository orderRepository) {
        this.tasklingClient = tasklingClient;
        this.orderRepository = orderRepository;
    }

    public void processF() {
        try (TaskExecutionContext executionContext = tasklingClient.createTaskExecutionContext("OrdersProcessor", "ProcessF")) {
            System.out.println("WILL TRY START TASK");
            if(executionContext.tryStart()) {
                System.out.println("STARTED TASK");

                ListBlockResponse<Order> response = getListBlocksSafe(executionContext);
                for(ListBlockContext<Order> blockContext : response.getBlockContexts()) {
                    blockContext.start();
                    System.out.println("STARTED BLOCK");

                    for(ListBlockItem<Order> orderItem : blockContext.getItems(ItemStatus.Pending)) {
                        Order order = orderItem.getValue();
                        System.out.println(MessageFormat.format("OrderId: {0,number,#}, CustomerId: {1,number,#}, OrderDate: {2}, Total: {3,number,#.##}",
                                order.getOrderId(),
                                order.getCustomerId(),
                                order.getOrderDate(),
                                order.getTotalOrderValue()));

                        orderItem.completed();
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

    public void processG() {
        try (TaskExecutionContext executionContext = tasklingClient.createTaskExecutionContext("OrdersProcessor", "ProcessG")) {
            System.out.println("WILL TRY START TASK");
            if(executionContext.tryStart()) {
                System.out.println("STARTED TASK");

                ListBlockResponse<Order> response = getListBlocksSafe(executionContext);
                for(ListBlockContext<Order> blockContext : response.getBlockContexts()) {
                    blockContext.start();
                    System.out.println("STARTED BLOCK");

                    for(ListBlockItem<Order> orderItem : blockContext.getItems(ItemStatus.Pending)) {
                        Order order = orderItem.getValue();
                        System.out.println(MessageFormat.format("OrderId: {0,number,#}, CustomerId: {1,number,#}, OrderDate: {2}, Total: {3,number,#.##}",
                                order.getOrderId(),
                                order.getCustomerId(),
                                order.getOrderDate(),
                                order.getTotalOrderValue()));

                        orderItem.completed();
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

    private ListBlockResponse getListBlocksSafe(TaskExecutionContext executionContext) {
        CriticalSectionContext cst = executionContext.createCriticalSection();

        if (cst.tryStart(Duration.ofSeconds(2), 30)) {
            try {
                List<Order> orders = orderRepository.getAllOrders();
                return executionContext.getListBlocks(Order.class, x -> x.withPeriodicCommit(orders, (short)100, BatchSize.Ten));
            }
            finally {
                cst.complete();
            }
        } else {
            System.out.println("COULD NOT ENTER CRITICAL SECTION");
            throw new RuntimeException("Could not safely generate blocks");
        }
    }

    private ListBlockWithHeaderResponse<Order, NumericRange> getListBlocksWithHeaderSafe(TaskExecutionContext executionContext) {
        CriticalSectionContext cst = executionContext.createCriticalSection();

        if (cst.tryStart(Duration.ofSeconds(2), 30)) {
            try {
                int maxOrderId = orderRepository.getMaxOrderId();
                ListBlockWithHeader<Order, NumericRange> lastListBlockProcessed = executionContext.getLastListBlockWithHeader(Order.class, NumericRange.class);

                NumericRange numericRange = new NumericRange(lastListBlockProcessed.getHeader().getTo()+1, maxOrderId);
                List<Order> orders = orderRepository.getOrdersById((int)numericRange.getFrom(), (int)numericRange.getTo());

                return executionContext.getListBlocksWithHeader(Order.class, NumericRange.class, x -> x.withPeriodicCommit(orders, numericRange, (short)100, BatchSize.Ten));
            }
            finally {
                cst.complete();
            }
        } else {
            System.out.println("COULD NOT ENTER CRITICAL SECTION");
            throw new RuntimeException("Could not safely generate blocks");
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
