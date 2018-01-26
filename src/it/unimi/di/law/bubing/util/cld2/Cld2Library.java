package it.unimi.di.law.bubing.util;

import com.sun.jna.Library;

/*
 * Copyright 2015-2017 EntIT Software LLC, a Micro Focus company.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.ptr.PointerByReference;

/**
 * JNA Interface to access the C++ CLD2 methods
 */
public interface Cld2Library extends Library {
    /**
     * this is the method used in CLD2 to detect the language, passing in the hints and
     * references (as java arrays) of the text buffer, language3, percent3, text_bytes and is_reliable fields
     *
     * @param buffer - Bytes of text to be queried
     * @param buffer_length - The length of buffer
     * @param is_plain_text - States whether the text passed in is plain-text or not
     * @param tld_hint - Encoding hint, it is from an encoding detector applied to an input
     * @param encoding_hint - Detector hint, such as "en" "en,it", ENGLISH
     * @param language_hint - The language code specifying a possible language that may be in the text.
     * @param language3 - Contains the top three languages found
     * @param percent3 - The confidence percentage for each of the top three languages found
     * @param text_bytes - The amount of non-tag/letters-only text found
     * @param is_reliable - Determines the confidence in the findings - True if the returned language is some amount more probable than
     *                      the second best.
     *
     * @return integer value for language corresponding to value in Cld2Language.
     *
     * language3 array contains the top three languages found
     * percent3 array contains the confidence for each of the top three languages found, that they are correct
     * text_bytes array contains the amount of non-tag/letters-only text found
     * is_reliable, which is set true if the returned language is some amount more probable than the second best
     * language. Calculation is a complex function of the length of the text and the different-script runs of text.
     */
    String JNA_LIBRARY_NAME = "cld2";
    NativeLibrary JNA_NATIVE_LIB = NativeLibrary.getInstance(Cld2Library.JNA_LIBRARY_NAME);
    Cld2Library INSTANCE = (Cld2Library) Native.loadLibrary(Cld2Library.JNA_LIBRARY_NAME,
            Cld2Library.class);


    //String LanguageName(int lang);
    String _ZN4CLD212LanguageNameENS_8LanguageE(int lang);

    //String LanguageCode(int lang);
    String _ZN4CLD212LanguageCodeENS_8LanguageE(int lang);

    //int GetLanguageFromName(String src);
    int _ZN4CLD219GetLanguageFromNameEPKc(String src);

    //int ExtDetectLanguageSummary(String buffer, int buffer_length, byte is_plain_text, CLDHints cld_hints, int flags, IntBuffer language3, IntBuffer percent3, DoubleBuffer normalized_score3, PointerByReference resultchunkvector, IntBuffer text_bytes, ByteBuffer is_reliable);
    int _ZN4CLD224ExtDetectLanguageSummaryEPKcibPKNS_8CLDHintsEiPNS_8LanguageEPiPdPSt6vectorINS_11ResultChunkESaISA_EES7_Pb(
            byte[] buffer, int bufferLength, boolean isPlainText, CldHints cldHints, int flags,
            int[] language3, int[] percent3, double[] normalizedScore3,
            PointerByReference resultchunkvector, int[] textBytes, boolean[] isReliable);

    //String DetectLanguageVersion();
    String _ZN4CLD221DetectLanguageVersionEv();
}
