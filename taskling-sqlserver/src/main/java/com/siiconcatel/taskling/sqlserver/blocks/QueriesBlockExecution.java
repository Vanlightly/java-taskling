package com.siiconcatel.taskling.sqlserver.blocks;

class QueriesBlockExecution {
    public static final String SetBlockExecutionStatusToStarted =
        "UPDATE [Taskling].[BlockExecution]\n" +
        "SET [BlockExecutionStatus] = :blockExecutionStatus\n" +
        ",[StartedAt] = GETUTCDATE()\n" +
        "WHERE BlockExecutionId = :blockExecutionId ";

    public static final String SetRangeBlockExecutionAsCompleted =
            "UPDATE [Taskling].[BlockExecution]\n" +
            "SET [CompletedAt] = GETUTCDATE()\n" +
            ",[BlockExecutionStatus] = :blockExecutionStatus\n" +
            ",[ItemsCount] = :itemsCount\n" +
            "WHERE BlockExecutionId = :blockExecutionId ";

    public static final String SetListBlockExecutionAsCompleted =
            "UPDATE [Taskling].[BlockExecution]\n" +
            "SET [CompletedAt] = GETUTCDATE()\n" +
            ",[BlockExecutionStatus] = :blockExecutionStatus\n" +
            "WHERE BlockExecutionId = :blockExecutionId ";
}
