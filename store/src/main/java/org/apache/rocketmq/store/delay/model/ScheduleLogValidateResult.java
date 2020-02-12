package org.apache.rocketmq.store.delay.model;

public class ScheduleLogValidateResult {

    private long maxScheduleLogOffset;

    private long maxCommitLogOffset;

    public long getMaxScheduleLogOffset() {
        return maxScheduleLogOffset;
    }

    public void setMaxScheduleLogOffset(long maxScheduleLogOffset) {
        this.maxScheduleLogOffset = maxScheduleLogOffset;
    }

    public long getMaxCommitLogOffset() {
        return maxCommitLogOffset;
    }

    public void setMaxCommitLogOffset(long maxCommitLogOffset) {
        this.maxCommitLogOffset = maxCommitLogOffset;
    }
}
