package com.microservices.elasticsearch.dynamic.query.exception;

public class InvalidOperatorException extends RuntimeException {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public InvalidOperatorException(String message) {
        super(message);
    }
}
