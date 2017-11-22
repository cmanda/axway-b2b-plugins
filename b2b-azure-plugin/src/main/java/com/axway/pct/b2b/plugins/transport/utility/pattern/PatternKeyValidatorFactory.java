package com.axway.pct.b2b.plugins.transport.utility.pattern;

public final class PatternKeyValidatorFactory {

	private static final String GLOB_TYPE = "glob";

	public static PatternKeyValidator createPatternValidator(String patternType) {
		if (patternType.equalsIgnoreCase(GLOB_TYPE)) {
			return new GlobValidator();
		}
		return new RegexValidator();
	}

}
