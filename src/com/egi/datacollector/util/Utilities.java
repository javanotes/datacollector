package com.egi.datacollector.util;

public class Utilities {
	
	private Utilities(){}
	
	private static int getOptionPos(String flag, String[] options) {
	    if (options == null)
	      return -1;
	    
	    for (int i = 0; i < options.length; i++) {
	      if ((options[i].length() > 0) && (options[i].charAt(0) == '-')) {
		// Check if it is a negative number
		try {
		  Double.valueOf(options[i]);
		} 
		catch (NumberFormatException e) {
		  // found?
		  if (options[i].equals("-" + flag))
		    return i;
		  // did we reach "--"?
		  if (options[i].charAt(1) == '-')
		    return -1;
		}
	      }
	    }
	    
	    return -1;
	  }
	
	/**
	   * Gets an option indicated by a flag "-String" from the given array
	   * of strings. Stops searching at the first marker "--". Replaces 
	   * flag and option with empty strings.
	   *
	   * @param flag the String indicating the option.
	   * @param options the array of strings containing all the options.
	   * @return the indicated option or an empty string
	   * @exception Exception if the option indicated by the flag can't be found
	   */
	public static String getOption(String flag, String[] options) 
		     {

		    String newString;
		    int i = getOptionPos(flag, options);

		    if (i > -1) {
		      if (options[i].equals("-" + flag)) {
			if (i + 1 == options.length) {
			  return "";
			}
			options[i] = "";
			newString = new String(options[i + 1]);
			options[i + 1] = "";
			return newString;
		      }
		      if (options[i].charAt(1) == '-') {
			return "";
		      }
		    }
		    
		    return "";
	}
	
	public static boolean isNullOrBlank(String string){
		return string == null || string.trim().equals("");
	}

}
