package it.unimi.di.law.bubing.util;

import com.google.common.primitives.Longs;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class IntCountMinSketchAtomicTest {

    @Test
    public void testSketch() {
        int s = (int) (1 * 1024 * 1024);
        IntCountMinSketchAtomic cms = new IntCountMinSketchAtomic((int)s, 3);
        ConcurrentCountingMap ccm = new ConcurrentCountingMap(1);

        Random rng = new Random();

        ArrayList<Integer> indexes = new ArrayList<Integer>();
        long[] data = new long[s];
        double[] values = new double[s];
        long[] negdata = new long[s];
        int maxVal = 100;
        for (int i = 0; i < s; i++) {
            long v = rng.nextLong();
            data[i] = v;
            indexes.add(i);
            values[i] = Math.abs(rng.nextDouble()); // Generate uniform distribution from 0 to 99
        }
        for (int i = 0; i < s; i++) {
            long v = rng.nextLong();
            negdata[i] = v;
        }

        System.out.println("Incrementing " + s + " items in counting maps with values 0 - " + maxVal);
        System.gc();
        double error = 0.0;
        long count = 0;

        long startCCMTime = System.nanoTime();
        for (int p = 0; p < maxVal; p++)
            count += indexes.parallelStream().mapToInt(i ->
            {
                ThreadLocalRandom trng = ThreadLocalRandom.current();
                if (trng.nextDouble() < values[i]) {
                    ccm.addTo(Longs.toByteArray(data[i]), 1);
                    return 1;
                } else return 0;
            }).sum();
        long endCCMTime = System.nanoTime();

        System.out.println("CCM add total : " + (double)(endCCMTime-startCCMTime)/1000000000.0 + "s ; per increment " + (double)(endCCMTime-startCCMTime)/count);
        System.out.println("Estimated CCM object size : " + ObjectSizeCalculator.getObjectSize(ccm));

        long startGetCCMTime = System.nanoTime();
        error = indexes.parallelStream().mapToDouble(i -> {
                double e = maxVal * values[i] - ccm.get(Longs.toByteArray(data[i]));
                return e * e;
            }).sum();

        System.gc();
        long endGetCCMTime = System.nanoTime();
        System.out.println("CCM Error get " + Math.sqrt(error)/s);
        System.out.println("CCM get per item " + (double)(endGetCCMTime-startGetCCMTime)/s);

        long startNegGetCCMTime = System.nanoTime();

        error = indexes.parallelStream().mapToDouble(i -> {
                    double e = ccm.get(Longs.toByteArray(negdata[i]));
                    return e * e;
            }).sum();
        System.out.println("CCM Error get NEG " + Math.sqrt(error)/s);

        System.gc();
        long endNegGetCCMTime = System.nanoTime();
        System.gc();
        System.out.println("CCM neg get per item " + (double)(endNegGetCCMTime-startNegGetCCMTime)/s);

        // CMS
        System.out.println("Incrementing " + s + " items in count min sketch with values 0 - " + maxVal);

        long startRHHSTime = System.nanoTime();
        count = 0;
        for (int p = 0; p < maxVal; p++)
            count += indexes.parallelStream().mapToInt(i ->
            {
                ThreadLocalRandom trng = ThreadLocalRandom.current();
                if (trng.nextDouble() < values[i]) {
                    cms.increment(Longs.toByteArray(data[i]));
                    return 1;
                } else return 0;
            }).sum();
        long endRHHSTime = System.nanoTime();
        System.out.println("CMS add total : " + (double)(endRHHSTime-startRHHSTime)/1000000000.0 + "s ; per increment " + (double)(endRHHSTime-startRHHSTime)/count);
        System.out.println("Estimated CMS object size : " + ObjectSizeCalculator.getObjectSize(cms));
        long startGetRHHSTime = System.nanoTime();
        error = indexes.parallelStream().mapToDouble(i -> {
            double e = maxVal * values[i] - cms.get(Longs.toByteArray(data[i]));
            return e * e;
        }).sum();

        System.gc();
        long endGetRHHSTime = System.nanoTime();
        System.out.println("CMS Error get " + Math.sqrt(error)/s);
        System.out.println("CMS get per item " + (double)(endGetRHHSTime-startGetRHHSTime)/s);

        long startNegGetRHHSTime = System.nanoTime();
        error = indexes.parallelStream().mapToDouble(i -> {
            double e = cms.get(Longs.toByteArray(negdata[i]));
            return e* e;
        }).sum();
        System.gc();
        long endNegGetRHHSTime = System.nanoTime();
        System.out.println("CMS Error get NEG " + Math.sqrt(error)/s);
        System.out.println("CMS neg get per item " + (double)(endNegGetRHHSTime-startNegGetRHHSTime)/s);
    }
}
