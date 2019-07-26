package com.adou.example.utils.kafka.core.exception;

public class IDNotUniqueException extends RuntimeException {
	private static final long serialVersionUID = -3199583750982713723L;

	public IDNotUniqueException() {
	}

	public IDNotUniqueException(String message, Throwable cause) {
		super(message, cause);
	}

	public IDNotUniqueException(String message) {
		super(message);
	}

	public IDNotUniqueException(Throwable cause) {
		super(cause);
	}
}
