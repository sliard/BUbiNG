package it.unimi.di.law.bubing.util;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.*;

public class MurmurHash3_128Test {

    @Test
    public void murmurhash3_x64_128() {
        int s = 100000;
        char [] data = new char[s];
        Random rng = new Random();

        for (int run = 0; run < 10; run++) {
            for (int i = 0; i < s; i++) data[i] = (char) rng.nextInt();
            MurmurHash3_128.LongPair r0 = new MurmurHash3_128.LongPair();
            MurmurHash3_128.LongPair r1 = new MurmurHash3_128.LongPair();
            MurmurHash3_128.murmurhash3_x64_128(data, 1234, r0);
            MurmurHash3_128.murmurhash3_x64_128_init(1234, r1);

            MurmurHash3_128.murmurhash3_x64_128_update(data, 0, 16*1024, r1);
            MurmurHash3_128.murmurhash3_x64_128_update(data, 16*1024, data.length - 16*1024, r1);
            MurmurHash3_128.murmurhash3_x64_128_finalize(data.length, r1);
            assertTrue((r0.val1 == r1.val1) && (r0.val2 == r1.val2));
        }
    }


}
