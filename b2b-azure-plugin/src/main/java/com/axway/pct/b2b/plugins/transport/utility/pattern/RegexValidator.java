package com.axway.pct.b2b.plugins.transport.utility.pattern;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexValidator implements PatternKeyValidator {

	@Override
	public boolean isValid(String input, String pattern) {
		Pattern regexPattern = Pattern.compile(pattern);
	    Matcher matcher = regexPattern.matcher(input);

	    return matcher.matches();
	}

}
