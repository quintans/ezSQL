package com.github.quintans.ezSQL.toolkit.io;

import java.io.UnsupportedEncodingException;

public class TextStore extends BinStore {
    private static final long serialVersionUID = 1L;

    /**
     * Default content charset to be used when no explicit charset
     * parameter is provided by the sender. Media subtypes of the
     * "text" type are defined to have a default charset value of
     * "ISO-8859-1" when received via HTTP.
     */
    //public static final String DEFAULT_CHARSET = "ISO-8859-1";
    // PQP - UTF8 rules :P
    public static final String DEFAULT_CHARSET = "UTF-8";
    
    public TextStore(){
    }
    
    public TextStore(int sizeThreshold) {
        super(sizeThreshold);
    }
    
    public TextStore(String string){
        set(string);
    }
    
    public TextStore(int sizeThreshold, byte[] data) {
        super(sizeThreshold);
        set(data);
    }
    
    /**
     * Sets the contents using a String as a source
     * 
     * @param string The string with the data
     */
    public void set(String string){
    	try {
			set(string.getBytes(DEFAULT_CHARSET));
		} catch (UnsupportedEncodingException e) {
			set(string.getBytes());
		}
    }

    
    /**
     * Sets the contents using a String as a source
     * 
     * @param string The string with the data
     * @param charset The Charset of the string
     * @throws UnsupportedEncodingException
     */
    public void set(String string, final String charset) throws UnsupportedEncodingException{
		set(string.getBytes(charset));
    }
     
    /**
     * Returns the contents of the file as a String, using the specified
     * encoding.  This method uses {@link #get()} to retrieve the
     * contents of the file.
     *
     * @param charset The charset to use.
     *
     * @return The contents of the file, as a string.
     *
     * @throws UnsupportedEncodingException if the requested character
     *                                      encoding is not available.
     */
    public String getString(final String charset) throws UnsupportedEncodingException {
        return new String(get(), charset);
    }


    /**
     * Returns the contents of the file as a String, using the default
     * character encoding.  This method uses {@link #get()} to retrieve the
     * contents of the file.
     *
     * @return The contents of the file, as a string.
     */
    // TODO Consider making this method throw UnsupportedEncodingException.
    public String getString() {
        byte[] rawdata = get();
        try {
            return new String(rawdata, DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            return new String(rawdata);
        }
    }



}
