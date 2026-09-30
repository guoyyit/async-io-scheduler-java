package com.github.guoyyit.asyncioschedulerjava.config;

import com.github.guoyyit.asyncioschedulerjava.model.OverflowPolicy;
import com.github.guoyyit.asyncioschedulerjava.model.QuotaAction;

import java.time.Duration;

/**
 * Configuration of the async IO scheduler. All values have safe defaults;
 * setters return {@code this} for fluent configuration.
 */
public final class SchedulerConfig {

    private int blockSize = 4096;
    private int maxPendingRequests = 1024;
    private int maxPendingPerSubmitter = 256;
    private OverflowPolicy overflowPolicy = OverflowPolicy.BLOCK;
    private int maxWriteBatchBlocks = 32;
    private Duration maxBatchDelay = Duration.ofMillis(20);
    private int cacheBlocks = 4096;
    private int prefetchStartBlocks = 2;
    private int prefetchMaxWindowBlocks = 16;
    private int prefetchTotalBudgetBlocks = 256;
    private int prefetchRandomThreshold = 3;
    private Duration prefetchIdleReset = Duration.ofMillis(100);
    private boolean fairScheduling = true;
    private Duration starvationAgeThreshold = Duration.ofMillis(50);
    private long readQuotaBytesPerSec = 0;
    private long writeQuotaBytesPerSec = 0;
    private QuotaAction quotaAction = QuotaAction.DELAY;
    private Duration quotaWindow = Duration.ofSeconds(1);

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public int getMaxPendingRequests() {
        return maxPendingRequests;
    }

    public void setMaxPendingRequests(int maxPendingRequests) {
        this.maxPendingRequests = maxPendingRequests;
    }

    public int getMaxPendingPerSubmitter() {
        return maxPendingPerSubmitter;
    }

    public void setMaxPendingPerSubmitter(int maxPendingPerSubmitter) {
        this.maxPendingPerSubmitter = maxPendingPerSubmitter;
    }

    public OverflowPolicy getOverflowPolicy() {
        return overflowPolicy;
    }

    public void setOverflowPolicy(OverflowPolicy overflowPolicy) {
        this.overflowPolicy = overflowPolicy;
    }

    public int getMaxWriteBatchBlocks() {
        return maxWriteBatchBlocks;
    }

    public void setMaxWriteBatchBlocks(int maxWriteBatchBlocks) {
        this.maxWriteBatchBlocks = maxWriteBatchBlocks;
    }

    public Duration getMaxBatchDelay() {
        return maxBatchDelay;
    }

    public void setMaxBatchDelay(Duration maxBatchDelay) {
        this.maxBatchDelay = maxBatchDelay;
    }

    public int getCacheBlocks() {
        return cacheBlocks;
    }

    public void setCacheBlocks(int cacheBlocks) {
        this.cacheBlocks = cacheBlocks;
    }

    public int getPrefetchStartBlocks() {
        return prefetchStartBlocks;
    }

    public void setPrefetchStartBlocks(int prefetchStartBlocks) {
        this.prefetchStartBlocks = prefetchStartBlocks;
    }

    public int getPrefetchMaxWindowBlocks() {
        return prefetchMaxWindowBlocks;
    }

    public void setPrefetchMaxWindowBlocks(int prefetchMaxWindowBlocks) {
        this.prefetchMaxWindowBlocks = prefetchMaxWindowBlocks;
    }

    public int getPrefetchTotalBudgetBlocks() {
        return prefetchTotalBudgetBlocks;
    }

    public void setPrefetchTotalBudgetBlocks(int prefetchTotalBudgetBlocks) {
        this.prefetchTotalBudgetBlocks = prefetchTotalBudgetBlocks;
    }

    public int getPrefetchRandomThreshold() {
        return prefetchRandomThreshold;
    }

    public void setPrefetchRandomThreshold(int prefetchRandomThreshold) {
        this.prefetchRandomThreshold = prefetchRandomThreshold;
    }

    public Duration getPrefetchIdleReset() {
        return prefetchIdleReset;
    }

    public void setPrefetchIdleReset(Duration prefetchIdleReset) {
        this.prefetchIdleReset = prefetchIdleReset;
    }

    public boolean isFairScheduling() {
        return fairScheduling;
    }

    public void setFairScheduling(boolean fairScheduling) {
        this.fairScheduling = fairScheduling;
    }

    public Duration getStarvationAgeThreshold() {
        return starvationAgeThreshold;
    }

    public void setStarvationAgeThreshold(Duration starvationAgeThreshold) {
        this.starvationAgeThreshold = starvationAgeThreshold;
    }

    public long getReadQuotaBytesPerSec() {
        return readQuotaBytesPerSec;
    }

    public void setReadQuotaBytesPerSec(long readQuotaBytesPerSec) {
        this.readQuotaBytesPerSec = readQuotaBytesPerSec;
    }

    public long getWriteQuotaBytesPerSec() {
        return writeQuotaBytesPerSec;
    }

    public void setWriteQuotaBytesPerSec(long writeQuotaBytesPerSec) {
        this.writeQuotaBytesPerSec = writeQuotaBytesPerSec;
    }

    public QuotaAction getQuotaAction() {
        return quotaAction;
    }

    public void setQuotaAction(QuotaAction quotaAction) {
        this.quotaAction = quotaAction;
    }

    public Duration getQuotaWindow() {
        return quotaWindow;
    }

    public void setQuotaWindow(Duration quotaWindow) {
        this.quotaWindow = quotaWindow;
    }
}
