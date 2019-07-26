package com.siiconcatel.taskling.sqlserverexamples;

import com.siiconcatel.taskling.core.TasklingClient;
import com.siiconcatel.taskling.sqlserver.SqlServerTasklingClient;
import com.siiconcatel.taskling.sqlserverexamples.batchjobs.MyConfigReader;
import com.siiconcatel.taskling.sqlserverexamples.batchjobs.blocks.BatchProcessWithDateBlocks;
import com.siiconcatel.taskling.sqlserverexamples.batchjobs.blocks.BatchProcessWithListBlocks;
import com.siiconcatel.taskling.sqlserverexamples.batchjobs.blocks.BatchProcessWithNumericBlocks;
import com.siiconcatel.taskling.sqlserverexamples.batchjobs.criticalsections.BatchProcessWithCs;
import com.siiconcatel.taskling.sqlserverexamples.batchjobs.loggingonly.BatchProcessOnlyLogging;
import com.siiconcatel.taskling.sqlserverexamples.businesslogic.OrderRepository;

public class Main {
    public static void main(String[] args) {
        Main main = new Main();
        main.listBlocks();
    }

    TasklingClient tasklingClient;
    OrderRepository orderRepository;

    public Main() {
        tasklingClient = new SqlServerTasklingClient(new MyConfigReader());
        orderRepository = new OrderRepository();
    }


    private void loggingOnly() {
        BatchProcessOnlyLogging batchProcess = new BatchProcessOnlyLogging(tasklingClient, orderRepository);
        batchProcess.process();
    }

    private void loggingOnlyDemoConcurrencyControl() {
        Runnable runnableBatchProc =
                () -> {
                    System.out.println("STARTING");
                    BatchProcessOnlyLogging batchProcess = new BatchProcessOnlyLogging(tasklingClient, orderRepository);
                    batchProcess.process();
                    System.out.println("COMPLETE");
                };

        Thread thread1 = new Thread(runnableBatchProc);
        thread1.start();

        Thread thread2 = new Thread(runnableBatchProc);
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }


    }

    private void csWithoutTimeout() {
        Runnable runnableBatchProc =
                () -> {
                    System.out.println("STARTING");
                    BatchProcessWithCs batchProcess = new BatchProcessWithCs(tasklingClient, orderRepository);
                    batchProcess.processA();
                    System.out.println("COMPLETE");
                };

        Thread thread1 = new Thread(runnableBatchProc);
        thread1.start();

        Thread thread2 = new Thread(runnableBatchProc);
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void csWithTimeout() {
        Runnable runnableBatchProc =
                () -> {
                    System.out.println("STARTING");
                    BatchProcessWithCs batchProcess = new BatchProcessWithCs(tasklingClient, orderRepository);
                    batchProcess.processB();
                    System.out.println("COMPLETE");
                };

        Thread thread1 = new Thread(runnableBatchProc);
        thread1.start();

        Thread thread2 = new Thread(runnableBatchProc);
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void dateBlocks() {
        BatchProcessWithDateBlocks batchProcess = new BatchProcessWithDateBlocks(tasklingClient, orderRepository);
        batchProcess.processC();
    }

    private void concurrentDateBlocks() {
        Runnable runnableBatchProc =
                () -> {
                    System.out.println("STARTING");
                    BatchProcessWithDateBlocks batchProcess = new BatchProcessWithDateBlocks(tasklingClient, orderRepository);
                    batchProcess.processC();
                    System.out.println("COMPLETE");
                };

        Thread thread1 = new Thread(runnableBatchProc);
        thread1.start();

        Thread thread2 = new Thread(runnableBatchProc);
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void concurrentDateBlocksSafe() {
        Runnable runnableBatchProc =
                () -> {
                    System.out.println("STARTING");
                    BatchProcessWithDateBlocks batchProcess = new BatchProcessWithDateBlocks(tasklingClient, orderRepository);
                    batchProcess.processD();
                    System.out.println("COMPLETE");
                };

        Thread thread1 = new Thread(runnableBatchProc);
        thread1.start();

        Thread thread2 = new Thread(runnableBatchProc);
        thread2.start();

        try {
            thread1.join();
            thread2.join();
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void numericBlocks() {
        BatchProcessWithNumericBlocks batchProcess = new BatchProcessWithNumericBlocks(tasklingClient, orderRepository);
        batchProcess.processE();
    }

    private void listBlocks() {
        BatchProcessWithListBlocks batchProcess = new BatchProcessWithListBlocks(tasklingClient, orderRepository);
        batchProcess.processF();
    }
}
