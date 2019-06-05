package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.common;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRequestBase;
import com.siiconcatel.taskling.core.tasks.ReprocessOption;

public class FindBlocksOfTaskRequest extends BlockRequestBase {
    public FindBlocksOfTaskRequest(TaskId taskId,
                                   String taskExecutionId,
                                   BlockType blockType,
                                   String referenceValueOfTask,
                                   ReprocessOption reprocessOption)
    {
        super(taskId, taskExecutionId, blockType);
        this.referenceValueOfTask = referenceValueOfTask;
        this.reprocessOption = reprocessOption;
    }

    private String referenceValueOfTask;
    private ReprocessOption reprocessOption;

    public String getReferenceValueOfTask() {
        return referenceValueOfTask;
    }

    public void setReferenceValueOfTask(String referenceValueOfTask) {
        this.referenceValueOfTask = referenceValueOfTask;
    }

    public ReprocessOption getReprocessOption() {
        return reprocessOption;
    }

    public void setReprocessOption(ReprocessOption reprocessOption) {
        this.reprocessOption = reprocessOption;
    }
}
