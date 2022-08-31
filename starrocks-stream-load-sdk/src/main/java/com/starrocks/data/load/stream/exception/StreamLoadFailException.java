package com.starrocks.data.load.stream.exception;

import com.starrocks.data.load.stream.StreamLoadResponse;

public class StreamLoadFailException extends RuntimeException {
    private StreamLoadResponse.StreamLoadResponseBody responseBody;

    public StreamLoadFailException(StreamLoadResponse.StreamLoadResponseBody responseBody) {
        this(responseBody.toString(), responseBody);
    }

    public StreamLoadFailException(String message) {
        this(message, null);
    }

    public StreamLoadFailException(String message, StreamLoadResponse.StreamLoadResponseBody responseBody) {
        super(message);
        this.responseBody = responseBody;
    }

    public StreamLoadFailException(Throwable e) {
        super(e);
    }
}
