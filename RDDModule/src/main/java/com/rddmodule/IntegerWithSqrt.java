package com.rddmodule;

public class IntegerWithSqrt {

    private int originalNumber;
    private double sqrt;

    public IntegerWithSqrt(int i) {
        this.originalNumber = i;
        this.sqrt = Math.sqrt(i);
    }
}