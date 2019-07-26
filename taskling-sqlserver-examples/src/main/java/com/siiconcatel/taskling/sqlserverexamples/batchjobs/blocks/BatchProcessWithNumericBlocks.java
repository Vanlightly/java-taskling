package com.siiconcatel.taskling.sqlserverexamples.batchjobs.blocks;

import com.siiconcatel.taskling.core.TasklingClient;
import com.siiconcatel.taskling.core.blocks.common.LastBlockOrder;
import com.siiconcatel.taskling.core.blocks.rangeblocks.DateRangeBlock;
import com.siiconcatel.taskling.core.blocks.rangeblocks.DateRangeBlockResponse;
import com.siiconcatel.taskling.core.blocks.rangeblocks.NumericRangeBlock;
import com.siiconcatel.taskling.core.blocks.rangeblocks.NumericRangeBlockResponse;
import com.siiconcatel.taskling.core.contexts.CriticalSectionContext;
import com.siiconcatel.taskling.core.contexts.DateRangeBlockContext;
import com.siiconcatel.taskling.core.contexts.NumericRangeBlockContext;
import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.sqlserverexamples.businesslogic.OrderRepository;
import com.siiconcatel.taskling.sqlserverexamples.model.Order;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

public class BatchProcessWithNumericBlocks {
    private TasklingClient tasklingClient;
    private OrderRepository orderRepository;

    public BatchProcessWithNumericBlocks(TasklingClient tasklingClient,
                                         OrderRepository orderRepository) {
        this.tasklingClient = tasklingClient;
        this.orderRepository = orderRepository;
    }

    public void processE() {
        try (TaskExecutionContext executionContext = tasklingClient.createTaskExecutionContext("OrdersProcessor", "ProcessE")) {
            System.out.println("WILL TRY START TASK");
            if(executionContext.tryStart()) {
                System.out.println("STARTED TASK");

                NumericRangeBlockResponse response = getNumericBlocksSafe(executionContext);
                for(NumericRangeBlockContext blockContext : response.getBlockContexts()) {
                    blockContext.start();
                    System.out.println("STARTED BLOCK");

                    List<Order> orders = orderRepository.getOrdersById(
                            (int)blockContext.getNumericRangeBlock().getStartNumber(),
                            (int)blockContext.getNumericRangeBlock().getEndNumber());

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

    private long getLastEndNumber(TaskExecutionContext executionContext) {
        System.out.println("GETTING LAST END DATE");
        NumericRangeBlock block = executionContext.getLastNumericRangeBlock(LastBlockOrder.LastCreated);
        if(block == null)
            return 0;
        else
            return block.getEndNumber();
    }

    private NumericRangeBlockResponse getNumericBlocksSafe(TaskExecutionContext executionContext) {
        CriticalSectionContext cst = executionContext.createCriticalSection();

        if (cst.tryStart(Duration.ofSeconds(2), 30)) {
            try {
                int lastEndId = (int)getLastEndNumber(executionContext);
                int maxOrderId = orderRepository.getMaxOrderId();

                if(lastEndId == maxOrderId) {
                    System.out.println("NO NEW DATA TO PROCESS");
                    return new NumericRangeBlockResponse(new ArrayList<>());
                }
                else {
                    System.out.println("LAST PROCESSED ID IS: " + lastEndId + ", MAX ORDER ID IS: " + maxOrderId);
                    return executionContext.getNumericRangeBlocks(x -> x.withRange(lastEndId, maxOrderId, 100));
                }
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
