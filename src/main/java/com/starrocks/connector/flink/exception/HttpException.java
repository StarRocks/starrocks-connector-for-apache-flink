package com.starrocks.connector.flink.exception;

public class HttpException extends Exception{
    public HttpException(String message) {
        super(message);
    }
}
