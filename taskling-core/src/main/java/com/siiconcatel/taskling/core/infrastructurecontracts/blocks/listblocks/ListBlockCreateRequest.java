package com.siiconcatel.taskling.core.infrastructurecontracts.blocks.listblocks;

import com.siiconcatel.taskling.core.blocks.common.BlockType;
import com.siiconcatel.taskling.core.infrastructurecontracts.TaskId;
import com.siiconcatel.taskling.core.infrastructurecontracts.blocks.BlockRequestBase;

import java.util.List;

public class ListBlockCreateRequest extends BlockRequestBase {
    public ListBlockCreateRequest(TaskId taskId,
                                  String taskExecutionId,
                                  List<String> serializedValues)
    {
        super(taskId, taskExecutionId, BlockType.List);
        this.serializedValues = serializedValues;
    }

    public ListBlockCreateRequest(TaskId taskId,
                                  String taskExecutionId,
                                  List<String> serializedValues,
                                  String serializedHeader,
                                  int compressionThreshold)
    {
        super(taskId, taskExecutionId, BlockType.List);
        this.serializedValues = serializedValues;
        this.serializedHeader = serializedHeader;
        this.compressionThreshold = compressionThreshold;
    }

    private List<String> serializedValues;
    private String serializedHeader;
    private int compressionThreshold;

    public List<String> getSerializedValues() {
        return serializedValues;
    }

    public void setSerializedValues(List<String> serializedValues) {
        this.serializedValues = serializedValues;
    }

    public String getSerializedHeader() {
        return serializedHeader;
    }

    public void setSerializedHeader(String serializedHeader) {
        this.serializedHeader = serializedHeader;
    }

    public int getCompressionThreshold() {
        return compressionThreshold;
    }

    public void setCompressionThreshold(int compressionThreshold) {
        this.compressionThreshold = compressionThreshold;
    }
}
