package it.unimi.di.law.bubing.util.detection;

public class LanguageDetectionInfo {
    public String httpHeaderLanguage="-";
    public String htmlLanguage="-";
    public String cld2Language="-";

    @Override
    public String toString() {
        return (httpHeaderLanguage == null ? "-" : httpHeaderLanguage) +
                "," + (htmlLanguage == null ? "-" : htmlLanguage) +
                ","+ (cld2Language == null ? "-" : cld2Language);
    }
}
