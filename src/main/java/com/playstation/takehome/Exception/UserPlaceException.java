package com.playstation.takehome.Exception;

public class UserPlaceException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public UserPlaceException() {
    }

    public UserPlaceException(String message) {
        super(message);
    }

    public UserPlaceException(Throwable cause) {
        super(cause);
    }

    public UserPlaceException(String message, Throwable cause) {
        super(message, cause);
    }

    public UserPlaceException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}