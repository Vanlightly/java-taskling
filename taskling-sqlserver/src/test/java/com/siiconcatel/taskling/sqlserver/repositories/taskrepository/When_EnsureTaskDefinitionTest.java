package com.siiconcatel.taskling.sqlserver.repositories.taskrepository;

import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskDefinition;
import com.siiconcatel.taskling.core.infrastructurecontracts.taskexecutions.TaskRepository;
import com.siiconcatel.taskling.sqlserver.categories.ExecutionTokenTests;
import com.siiconcatel.taskling.sqlserver.categories.FastTests;
import com.siiconcatel.taskling.sqlserver.categories.SlowTests;
import com.siiconcatel.taskling.sqlserver.categories.TaskExecutionTests;
import com.siiconcatel.taskling.sqlserver.helpers.ExecutionsHelper;
import com.siiconcatel.taskling.sqlserver.taskexecution.TaskRepositoryMsSql;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

public class When_EnsureTaskDefinitionTest {
    private ExecutionsHelper executionHelper;

    public When_EnsureTaskDefinitionTest()
    {
        executionHelper = new ExecutionsHelper();
    }

    private TaskRepository createSut()
    {
        return new TaskRepositoryMsSql();
    }

    @Category({SlowTests.class, TaskExecutionTests.class})
    @Test
    public void If_WhenEnsureTaskDefinitionCalledConcurrently_TaskIsCreatedOnceAndReturnedAsync()
    {
        // ARRANGE
        TaskRepository sut = createSut();

        // ACT
        for(int i=0; i<100; i++)
        {
            TaskId taskId = new TaskId(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            executionHelper.addConnection(taskId);
            List<TaskId> taskIds = new ArrayList<>();
            for (int t = 0; t < 10; t++)
                taskIds.add(taskId);

            Queue<TaskDefinition> globalQueue = new ConcurrentLinkedQueue<>();
            taskIds.parallelStream()
                    .forEach(x -> globalQueue.add(sut.ensureTaskDefinition(x))
            );

            // ASSERT
            // ensure that all definition ids are the same, this means that only one was created
            int definitionId = globalQueue.peek().getTaskDefinitionId();
            globalQueue.stream().allMatch(x -> x.getTaskDefinitionId() == definitionId);
        }


    }
}
