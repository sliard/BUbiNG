package it.unimi.di.law.bubing.util;

/**
 * Holds the result of calling detect.
 *
 * It has the name and code of the language and the confidence with which that language was
 * detected.
 */
public class CldResult {
    public final String language;
    public final String code;
    public final double confidence;

    public CldResult(String language, String code, double confidence) {
        this.language = language;
        this.code = code;
        this.confidence = confidence;
    }
}
