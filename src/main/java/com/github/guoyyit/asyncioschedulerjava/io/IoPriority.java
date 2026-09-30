package com.github.guoyyit.asyncioschedulerjava.io;

/**
 * IO request priority. Lower ordinal value means higher priority.
 */
public enum IoPriority {
    HIGH(0),
    NORMAL(1),
    LOW(2);

    private final int value;

    IoPriority(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }
}
