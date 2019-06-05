package com.siiconcatel.taskling.sqlserver.tokens.criticalsections;

import com.siiconcatel.taskling.core.utils.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class CriticalSectionState {

    private boolean isGranted;
    private boolean hasBeenModified;
    private String grantedToExecution;
    private List<CriticalSectionQueueItem> queue;


    public boolean isGranted()
    {
        return isGranted;
    }

    public void setGranted(boolean granted)
    {
        if (isGranted != granted)
            hasBeenModified = true;

        isGranted = granted;
    }


    public String getGrantedToExecution()
    {
        return grantedToExecution;
    }

    public void setGrantedToExecution(String value)
    {
        if (grantedToExecution != value)
            hasBeenModified = true;

        grantedToExecution = value;
    }

    public void startTrackingModifications()
    {
        hasBeenModified = false;
    }

    public boolean HasQueuedExecutions()
    {
        return queue != null && !queue.isEmpty();
    }

    public void setQueue(String queueStr)
    {
        queue = new ArrayList<>();
        if (!StringUtils.isNullOrEmpty(queueStr))
        {
            String[] queueItems = queueStr.split("\\|");
            for (String queueItem : queueItems)
            {
                if(!StringUtils.isNullOrEmpty(queueItem)) {
                    String[] parts = queueItem.split("\\,");
                    int index = Integer.parseInt(parts[0]);
                    queue.add(new CriticalSectionQueueItem(index, parts[1]));
                }
            }
        }
    }

    public String getQueueString()
    {
        StringBuilder sb = new StringBuilder();
        for (CriticalSectionQueueItem queueItem : queue)
        {
            if (queueItem.getIndex() > 1)
                sb.append("|");

            sb.append(queueItem.getIndex() + "," + queueItem.getTaskExecutionId());
        }

        return sb.toString();
    }

    public List<CriticalSectionQueueItem> getQueue()
    {
        return queue;
    }

    public void updateQueue(List<CriticalSectionQueueItem> queueDetails)
    {
        queue = queueDetails;
        hasBeenModified = true;
    }

    public String getFirstExecutionIdInQueue()
    {
        if (queue == null || queue.isEmpty())
            return "";

        return queue.stream()
                .sorted(Comparator.comparing(CriticalSectionQueueItem::getIndex))
                .findFirst()
                .get()
                .getTaskExecutionId();
    }

    public void removeFirstInQueue()
    {
        // remove the first element
        if (queue != null && !queue.isEmpty())
            queue.remove(0);

        // reset the index values
        int index = 1;
        List<CriticalSectionQueueItem> sorted = queue.stream()
                                                .sorted(Comparator.comparing(CriticalSectionQueueItem::getIndex))
                                                .collect(Collectors.toList());
        for (CriticalSectionQueueItem item : sorted)
        {
            item.setIndex(index);
            index++;
        }

        hasBeenModified = true;
    }

    public boolean existsInQueue(String taskExecutionId)
    {
        return queue.stream().anyMatch(x -> x.getTaskExecutionId() == taskExecutionId);
    }

    public void addToQueue(String taskExecutionId)
    {
        int index = 1;
        if (!queue.isEmpty())
            index = queue.stream().max(Comparator.comparing(CriticalSectionQueueItem::getIndex)).get().getIndex() + 1;

        queue.add(new CriticalSectionQueueItem(index, taskExecutionId));
        hasBeenModified = true;
    }

    public boolean hasBeenModified() {
        return hasBeenModified;
    }
}
