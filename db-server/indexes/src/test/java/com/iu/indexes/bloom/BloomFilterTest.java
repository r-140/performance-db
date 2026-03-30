package com.iu.indexes.bloom;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class BloomFilterTest {

    @Test
    void addedKey_alwaysFound() {
        BloomFilter bf = new BloomFilter(1000, 0.01);
        for (int i = 0; i < 100; i++) bf.add(i);
        for (int i = 0; i < 100; i++)
            assertTrue(bf.mightContain(i), "Key " + i + " was added so must be found");
    }

    @Test
    void noFalseNegatives_guaranteed() {
        BloomFilter bf = new BloomFilter(500, 0.01);
        IntStream.range(0, 500).forEach(bf::add);
        IntStream.range(0, 500).forEach(i ->
            assertTrue(bf.mightContain(i), "No false negatives allowed"));
    }

    @Test
    void falsePositiveRate_withinBound() {
        int n = 10_000;
        double targetFpr = 0.01;
        BloomFilter bf = new BloomFilter(n, targetFpr);
        IntStream.range(0, n).forEach(bf::add);

        // Check false positive rate on keys definitely NOT added
        long falsePositives = IntStream.range(n, 2 * n)
                .filter(bf::mightContain)
                .count();
        double actualFpr = (double) falsePositives / n;

        // Allow 3x the target (probabilistic; very unlikely to exceed)
        assertTrue(actualFpr <= targetFpr * 3,
            "False positive rate " + actualFpr + " exceeds 3x target " + targetFpr);
    }

    @Test
    void emptyFilter_returnsNotFound() {
        BloomFilter bf = new BloomFilter(100, 0.01);
        // An empty filter must return false for any key
        // (all bits 0 → first hash check returns false)
        assertFalse(bf.mightContain(42));
    }

    @Test
    void estimatedFpr_closeToTarget() {
        int n = 1000;
        double target = 0.01;
        BloomFilter bf = new BloomFilter(n, target);
        IntStream.range(0, n).forEach(bf::add);
        double estimated = bf.estimatedFalsePositiveRate();
        assertTrue(estimated < target * 2,
            "Estimated FPR " + estimated + " should be near target " + target);
    }
}
