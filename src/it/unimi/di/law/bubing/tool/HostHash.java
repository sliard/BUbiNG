package it.unimi.di.law.bubing.tool;
//RELEASE-STATUS: DIST

import it.unimi.di.law.bubing.util.MurmurHash3;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class HostHash {
    private static final Charset charset = StandardCharsets.UTF_8;

    public static long hostLongHash(String host) {
        byte[] tab = host.getBytes(charset);
        return (MurmurHash3.hash(tab));
    }
}
