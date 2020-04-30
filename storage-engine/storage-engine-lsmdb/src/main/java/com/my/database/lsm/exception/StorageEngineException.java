package com.my.database.lsm.exception;

public class StorageEngineException extends RuntimeException {

    public StorageEngineException(String message, final Object... args) {
        super(String.format(message, args));
    }
}
