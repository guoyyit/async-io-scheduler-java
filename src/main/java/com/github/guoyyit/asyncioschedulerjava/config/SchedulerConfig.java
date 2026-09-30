package com.github.guoyyit.asyncioschedulerjava.config;

import java.time.Duration;

/**
 * All tunable parameters of the async IO scheduler.
 */
public class SchedulerConfig {

    private int blockSize = 4096;
    private int maxPendingRequests = 1024;
    private int maxPendingPerSubmitter = 256;
    private int ioWorkers = 2;
    private Duration shutdownTimeout = Duration.ofSeconds(10);

    private FairnessPolicy fairnessPolicy = FairnessPolicy.AGING_STRICT;
    private Duration agingThreshold = Duration.ofMillis(500);
    private double highWeight = 4.0;
    private double normalWeight = 2.0;
    private double lowWeight = 1.0;

    private int maxBatchBytes = 64 * 1024;
    private Duration batchLinger = Duration.ofMillis(5);

    private boolean prefetchEnabled = true;
    private int prefetchMinBytes = 4096;
    private int prefetchMaxBytes = 64 * 1024;
    private int prefetchTotalBudgetBytes = 1024 * 1024;
    private double prefetchGrowthFactor = 2.0;
    private double prefetchBackoffFactor = 0.5;
    private int sequentialHitThreshold = 2;

    private OverQuotaAction overQuotaAction = OverQuotaAction.DELAY;
    private long defaultReadQuotaBytesPerSec = Long.MAX_VALUE;
    private long defaultWriteQuotaBytesPerSec = Long.MAX_VALUE;

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

    public int getIoWorkers() {
        return ioWorkers;
    }

    public void setIoWorkers(int ioWorkers) {
        this.ioWorkers = ioWorkers;
    }

    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    public void setShutdownTimeout(Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    public FairnessPolicy getFairnessPolicy() {
        return fairnessPolicy;
    }

    public void setFairnessPolicy(FairnessPolicy fairnessPolicy) {
        this.fairnessPolicy = fairnessPolicy;
    }

    public Duration getAgingThreshold() {
        return agingThreshold;
    }

    public void setAgingThreshold(Duration agingThreshold) {
        this.agingThreshold = agingThreshold;
    }

    public double getHighWeight() {
        return highWeight;
    }

    public void setHighWeight(double highWeight) {
        this.highWeight = highWeight;
    }

    public double getNormalWeight() {
        return normalWeight;
    }

    public void setNormalWeight(double normalWeight) {
        this.normalWeight = normalWeight;
    }

    public double getLowWeight() {
        return lowWeight;
    }

    public void setLowWeight(double lowWeight) {
        this.lowWeight = lowWeight;
    }

    public int getMaxBatchBytes() {
        return maxBatchBytes;
    }

    public void setMaxBatchBytes(int maxBatchBytes) {
        this.maxBatchBytes = maxBatchBytes;
    }

    public Duration getBatchLinger() {
        return batchLinger;
    }

    public void setBatchLinger(Duration batchLinger) {
        this.batchLinger = batchLinger;
    }

    public boolean isPrefetchEnabled() {
        return prefetchEnabled;
    }

    public void setPrefetchEnabled(boolean prefetchEnabled) {
        this.prefetchEnabled = prefetchEnabled;
    }

    public int getPrefetchMinBytes() {
        return prefetchMinBytes;
    }

    public void setPrefetchMinBytes(int prefetchMinBytes) {
        this.prefetchMinBytes = prefetchMinBytes;
    }

    public int getPrefetchMaxBytes() {
        return prefetchMaxBytes;
    }

    public void setPrefetchMaxBytes(int prefetchMaxBytes) {
        this.prefetchMaxBytes = prefetchMaxBytes;
    }

    public int getPrefetchTotalBudgetBytes() {
        return prefetchTotalBudgetBytes;
    }

    public void setPrefetchTotalBudgetBytes(int prefetchTotalBudgetBytes) {
        this.prefetchTotalBudgetBytes = prefetchTotalBudgetBytes;
    }

    public double getPrefetchGrowthFactor() {
        return prefetchGrowthFactor;
    }

    public void setPrefetchGrowthFactor(double prefetchGrowthFactor) {
        this.prefetchGrowthFactor = prefetchGrowthFactor;
    }

    public double getPrefetchBackoffFactor() {
        return prefetchBackoffFactor;
    }

    public void setPrefetchBackoffFactor(double prefetchBackoffFactor) {
        this.prefetchBackoffFactor = prefetchBackoffFactor;
    }

    public int getSequentialHitThreshold() {
        return sequentialHitThreshold;
    }

    public void setSequentialHitThreshold(int sequentialHitThreshold) {
        this.sequentialHitThreshold = sequentialHitThreshold;
    }

    public OverQuotaAction getOverQuotaAction() {
        return overQuotaAction;
    }

    public void setOverQuotaAction(OverQuotaAction overQuotaAction) {
        this.overQuotaAction = overQuotaAction;
    }

    public long getDefaultReadQuotaBytesPerSec() {
        return defaultReadQuotaBytesPerSec;
    }

    public void setDefaultReadQuotaBytesPerSec(long defaultReadQuotaBytesPerSec) {
        this.defaultReadQuotaBytesPerSec = defaultReadQuotaBytesPerSec;
    }

    public long getDefaultWriteQuotaBytesPerSec() {
        return defaultWriteQuotaBytesPerSec;
    }

    public void setDefaultWriteQuotaBytesPerSec(long defaultWriteQuotaBytesPerSec) {
        this.defaultWriteQuotaBytesPerSec = defaultWriteQuotaBytesPerSec;
    }
}
