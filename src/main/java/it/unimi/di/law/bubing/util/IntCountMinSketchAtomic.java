package it.unimi.di.law.bubing.util;

//RELEASE-STATUS: DIST

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class IntCountMinSketchAtomic implements Serializable, Cloneable {
    private int depth;
    private int width;
    private AtomicIntegerArray[] arrays;

    public IntCountMinSketchAtomic(int w, int d) {
        depth = d;
        width = w;
        arrays = new AtomicIntegerArray[d];
        for (int i=0; i<depth;i++)
            arrays[i] = new AtomicIntegerArray(width);

    }

    public int get(byte[] b) {
        return get(b, 0, b.length);
    }
    public int get(byte[] b, int offset, int length) {
        int val = Integer.MAX_VALUE;
        for (int i=0; i < depth; i++) {
            int c = arrays[i].get((int) (MurmurHash3.hash(b, offset, length, i) & 0x7FFFFFFF) % width);
            if (c < val)
                val = c;
        }
        return val;
    }

    public void put(byte[] b, int value) {
        put(b, 0, b.length, value);
    }

    public void put(byte[] b, int offset, int length, int value) {
        for (int i=0; i < depth; i++) {
            int pos = (int) (MurmurHash3.hash(b, offset, length, i) & 0x7FFFFFFF) % width;
            if (arrays[i].get(pos) < value)
                arrays[i].lazySet(pos, value);
        }
    }

    public int increment(byte[] b) {
        return increment(b, 0, b.length);
    }

    public int increment(byte[] b, int offset, int length) {
        boolean updated = false;
        int[] pos = new int[depth];
        for (int i = 0; i < depth; i++)
            pos[i] = (int) (MurmurHash3.hash(b, offset, length, i) & 0x7FFFFFFF) % width;
        int val = Integer.MAX_VALUE;
        while (!updated) { // need at least one successful update
            val = Integer.MAX_VALUE;
            for (int i=0; i < depth; i++) {
                int c = arrays[i].get(pos[i]);
                if (c < val)
                    val = c;
            }
            for (int i = 0; i < depth; i++) {
                updated |= arrays[i].weakCompareAndSet(pos[i], val, val + 1);
            }
        }
        return val + 1;
    }
    private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
        s.defaultWriteObject();
    }

    private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();
    }
}
