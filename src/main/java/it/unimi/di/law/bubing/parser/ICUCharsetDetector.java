package com.ibm.icu.text;


public class ICUCharsetDetector extends CharsetDetector
{
  public CharsetDetector setText( final byte[] in, final int length ) {
    this.fRawInput = in;
    this.fRawLength = length;
    return this;
  }
}
