package com.axway.pct.b2b.plugins.transport.utility.pattern;

public abstract interface PatternKeyValidator {
	
	public abstract boolean isValid(String input, String pattern);

}
