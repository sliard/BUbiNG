package it.unimi.di.law.bubing.util;

//RELEASE-STATUS: DIST

import java.io.Serializable;

public final class IntCountMinSketchUnsafe implements Serializable, Cloneable{
    private final int depth;
    private final int width;
    private final int[][] arrays;

    public IntCountMinSketchUnsafe(int w, int d) {
        depth = d;
        width = w;
        arrays = new int[d][];
        for (int i=0; i<depth;i++)
            arrays[i] = new int[width];
    }

    public final int get(final byte[] b) {
        return get(b, 0, b.length);
    }
    public final int get(final byte[] b, int offset, int length) {
        int val = Integer.MAX_VALUE;
        for (int i=0; i < depth; i++) {
            int c = arrays[i][(int) (MurmurHash3.hash(b, offset, length, i) & 0x7FFFFFFF) % width];
            if (c < val)
                val = c;
        }
        return val;
    }

    public final void put(final byte[] b, final int value) {
        put(b, 0, b.length, value);
    }

    public final void put(final byte[] b, final int offset, final int length, final int value) {
        for (int i=0; i < depth; i++) {
            int pos = (int) (MurmurHash3.hash(b, offset, length, i) & 0x7FFFFFFF) % width;
            if (arrays[i][pos] < value)
                arrays[i][pos] = value;
        }
    }

    public final int increment(final byte[] b) {
        return increment(b, 0, b.length);
    }

    public final int increment(final byte[] b, final int offset, final int length) {

        final int[] pos = new int[depth];
        for (int i = 0; i < depth; i++)
            pos[i] = (int) (MurmurHash3.hash(b, offset, length, i) & 0x7FFFFFFF) % width;
        int val = Integer.MAX_VALUE;
        for (int i=0; i < depth; i++) {
            int c = arrays[i][pos[i]];
            if (c < val)
                val = c;
        }
        if (val < Integer.MAX_VALUE) {
            for (int i = 0; i < depth; i++) {
                if (arrays[i][pos[i]] == val)
                    arrays[i][pos[i]] = val+1;
            }
            return val + 1;
        } else
            return val;
    }

    private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException {
        s.defaultWriteObject();
    }

    private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();
    }
}
