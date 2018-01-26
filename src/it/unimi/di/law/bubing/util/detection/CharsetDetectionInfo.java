package it.unimi.di.law.bubing.util.detection;

public class CharsetDetectionInfo {
    public String httpHeaderCharset="-";
    public String htmlMetaCharset="-";
    public String icuCharset="-";

    @Override
    public String toString() {
        return (httpHeaderCharset == null ? "-":httpHeaderCharset) + "," +
                (htmlMetaCharset  == null ? "-":htmlMetaCharset) + ","+
                (icuCharset == null ? "-":icuCharset);
    }
}
