package com.siiconcatel.taskling.sqlserverexamples.batchjobs.loggingonly;

import com.siiconcatel.taskling.core.TasklingClient;
import com.siiconcatel.taskling.core.contexts.TaskExecutionContext;
import com.siiconcatel.taskling.sqlserverexamples.businesslogic.OrderRepository;
import com.siiconcatel.taskling.sqlserverexamples.model.Order;

import java.text.MessageFormat;
import java.util.List;
import java.util.Random;

public class BatchProcessOnlyLogging {
    private TasklingClient tasklingClient;
    private OrderRepository orderRepository;

    public BatchProcessOnlyLogging(TasklingClient tasklingClient,
                                   OrderRepository orderRepository) {
        this.tasklingClient = tasklingClient;
        this.orderRepository = orderRepository;
    }

    public void process() {
        try (TaskExecutionContext executionContext = tasklingClient.createTaskExecutionContext("OrdersProcessor", "NewOrders")) {
            System.out.println("WILL TRY START");
            if(executionContext.tryStart()) {
                System.out.println("STARTED");
                List<Order> orders = orderRepository.getAllOrders();
                for(Order order : orders) {
                    System.out.println(MessageFormat.format("OrderId: {0,number,#}, CustomerId: {1,number,#}, OrderDate: {2}, Total: {3,number,#.##}",
                            order.getOrderId(),
                            order.getCustomerId(),
                            order.getOrderDate(),
                            order.getTotalOrderValue()));
                }

            }
            else {
                System.out.println("COULD NOT START");
            }
        }
    }
}
