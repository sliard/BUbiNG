package it.unimi.di.law.bubing.util;

public class Cld2Tool {
    /**
     * using jna, calls into the c++ code to retrieve the language code based on the integer language enum value.
     * the name of the C++ method is mangled
     * @param lang3
     * @return String[] containing the codes
     */
    public static String[] getLanguageCodes(Cld2Library cld2Library, int[] lang3){
        String[] codes = new String[3];
        for(int i=0; i<3; i++){
            codes[i] = cld2Library._ZN4CLD212LanguageCodeENS_8LanguageE(lang3[i]);
        }
        return codes;
    }


    /**
     * using jna, calls into the c++ code to retrieve the language name based on the integer language enum value.
     * @param lang3
     * @return String[] containing the language names
     */
    public static String[] getLanguageNames(Cld2Library cld2Library, int[] lang3){
        String[] names = new String[3];
        for(int i=0; i<3; i++){
            names[i] = cld2Library._ZN4CLD212LanguageNameENS_8LanguageE(lang3[i]);
        }
        return names;
    }


    /**
     * Public interface for the CLD2 library.
     */
        public static int getLanguageFromName(String name) {
            return Cld2Library.INSTANCE._ZN4CLD219GetLanguageFromNameEPKc(name);
        }

        public static String getLanguageName(int language) {
            return Cld2Library.INSTANCE._ZN4CLD212LanguageNameENS_8LanguageE(language);
        }

        public static String getLanguageCode(int language) {
            return Cld2Library.INSTANCE._ZN4CLD212LanguageCodeENS_8LanguageE(language);
        }

        public static String version() {
            return Cld2Library.INSTANCE._ZN4CLD221DetectLanguageVersionEv();
        }

        public static CldResult detect(String text) {
            boolean isPlainText = true;
            CldHints cldHints = new CldHints(
                    null,
                    "",
                    CldEncoding.UNKNOWN_ENCODING,
                    CldLanguage.UNKNOWN_LANGUAGE);
            int flags = 0;
            int[] language3 = new int[3];
            int[] percent3 = new int[3];
            double[] normalizedScore3 = new double[3];
            int[] textBytes = new int[1];
            boolean[] isReliable = new boolean[1];
            byte[] utf8EncodedText;
            try {
                utf8EncodedText = text.getBytes("UTF-8");
            } catch (java.io.UnsupportedEncodingException exc) {
                return new CldResult("UNKNOWN", "UNKNOWN", 0.0);
            }
            int language = Cld2Library.INSTANCE._ZN4CLD224ExtDetectLanguageSummaryEPKcibPKNS_8CLDHintsEiPNS_8LanguageEPiPdPSt6vectorINS_11ResultChunkESaISA_EES7_Pb(
                    utf8EncodedText,
                    utf8EncodedText.length,
                    isPlainText,
                    cldHints,
                    flags,
                    language3,
                    percent3,
                    normalizedScore3,
                    null, // Supposed to be a vector of ResultChunks, but it is not direct to pass vectors.
                    textBytes,
                    isReliable);

            return new CldResult(
                    getLanguageName(language), getLanguageCode(language), percent3[0] / 100.0);
        }
}
