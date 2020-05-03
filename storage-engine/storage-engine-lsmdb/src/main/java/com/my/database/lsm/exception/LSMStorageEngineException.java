package com.my.database.lsm.exception;

public class LSMStorageEngineException extends RuntimeException {

    public LSMStorageEngineException(String message, final Object... args) {
        super(String.format(message, args));
    }
}
