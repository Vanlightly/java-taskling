package com.siiconcatel.taskling.sqlserverexamples.businesslogic;

import com.siiconcatel.taskling.sqlserverexamples.model.Order;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

// this is just a fake repository/dao
public class OrderRepository {

    private static final int OrdersCount = 1000;

    public List<Order> getOrdersById(int fromId, int toId) {
        return getAllOrders().stream()
                .filter(x -> x.getOrderId() >= fromId && x.getOrderId() <= toId)
                .collect(Collectors.toList());
    }

    public List<Order> getOrdersByOrderDate(LocalDate fromDate, LocalDate toDate) {
        return getAllOrders().stream()
                .filter(x -> (x.getOrderDate().isEqual(fromDate) || x.getOrderDate().isAfter(fromDate))
                        && x.getOrderDate().isBefore(toDate))
                .collect(Collectors.toList());
    }

    public int getMaxOrderId() {
        return getAllOrders().stream()
                .map(x -> x.getOrderId())
                .max(Integer::compareTo)
                .get();
    }

    public List<Order> getAllOrders() {
        List<Order> orders = new ArrayList<>();
        LocalDate start = LocalDate.now().minusDays(365);

        for(int i=0; i<OrdersCount; i++) {
            Order order = new Order(i,
                    i,
                    start.plusDays(i % 365),
                    (i % 100)+1);
            orders.add(order);
        }

        return orders;
    }
}
