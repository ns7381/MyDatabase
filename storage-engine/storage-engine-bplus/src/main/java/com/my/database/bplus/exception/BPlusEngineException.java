package com.my.database.bplus.exception;

public class BPlusEngineException extends Exception {
    public BPlusEngineException(String exceptionType) {
        super(exceptionType);
    }

    public BPlusEngineException() {
        super("BPlus Storage Engine Exception");
    }
}
