package com.starrocks.data.load.stream.exception;

public class StreamLoadFailException extends RuntimeException {

    public StreamLoadFailException(String message) {
        super(message);
    }

    public StreamLoadFailException(String message, Throwable cause) {
        super(message, cause);
    }
}
