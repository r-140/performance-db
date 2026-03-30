package com.iu.indexes.gin;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generalized Inverted Index (GIN) implementation.
 *
 * GIN maps each token/term → sorted list of document offsets (posting list).
 * This is the standard structure used in full-text search engines (PostgreSQL GIN,
 * Lucene). Here tokens are extracted from JSON values by splitting on whitespace
 * and common delimiters, so queries like "find all docs containing 'testdata42'"
 * run in O(log T + k) instead of O(N).
 *
 * Disk layout  (ginindex.dat):
 *   One line per entry:  <token>|<offset1>,<offset2>,...
 *
 * The in-memory structure is a TreeMap for ordered traversal and range support.
 */
public class GINIndex {
    private static final Logger LOGGER = Logger.getLogger(GINIndex.class.getName());

    /** token → sorted list of file offsets */
    private final TreeMap<String, List<Long>> postingLists = new TreeMap<>();

    private static final String TOKEN_SEPARATOR   = "\\|";
    private static final String OFFSET_SEPARATOR  = ",";
    private static final String FILE_TOKEN_SEP    = "|";
    private static final String FILE_OFFSET_SEP   = ",";

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Index a document. The value string is tokenised and every token is mapped
     * to the given file offset.
     */
    public void insert(String value, long offset) {
        for (String token : tokenize(value)) {
            postingLists.computeIfAbsent(token, k -> new ArrayList<>()).add(offset);
        }
    }

    /**
     * Remove a specific offset from every posting list (used on document delete).
     */
    public void delete(long offset) {
        for (List<Long> list : postingLists.values()) {
            list.remove(offset);
        }
        // clean up empty lists
        postingLists.values().removeIf(List::isEmpty);
    }

    /**
     * Search for a single token. Returns all file offsets containing that token,
     * or an empty list if not found.
     */
    public List<Long> search(String token) {
        List<Long> result = postingLists.get(token.toLowerCase());
        return result != null ? Collections.unmodifiableList(result) : Collections.emptyList();
    }

    /**
     * Boolean AND across multiple tokens. Returns offsets present in ALL posting lists.
     */
    public List<Long> searchAll(List<String> tokens) {
        if (tokens.isEmpty()) return Collections.emptyList();

        Set<Long> result = null;
        for (String token : tokens) {
            List<Long> posting = postingLists.getOrDefault(token.toLowerCase(), Collections.emptyList());
            if (result == null) {
                result = new HashSet<>(posting);
            } else {
                result.retainAll(new HashSet<>(posting));
            }
            if (result.isEmpty()) return Collections.emptyList();
        }
        return result != null ? new ArrayList<>(result) : Collections.emptyList();
    }

    /**
     * Boolean OR across multiple tokens. Returns offsets present in ANY posting list.
     */
    public List<Long> searchAny(List<String> tokens) {
        Set<Long> result = new HashSet<>();
        for (String token : tokens) {
            result.addAll(postingLists.getOrDefault(token.toLowerCase(), Collections.emptyList()));
        }
        return new ArrayList<>(result);
    }

    /** Returns the number of distinct indexed tokens. */
    public int tokenCount() {
        return postingLists.size();
    }

    // -----------------------------------------------------------------------
    // Persistence
    // -----------------------------------------------------------------------

    /** Write the full index to disk. Format: token|offset1,offset2,... */
    public void saveToDisk(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            for (Map.Entry<String, List<Long>> entry : postingLists.entrySet()) {
                StringBuilder sb = new StringBuilder(entry.getKey()).append(FILE_TOKEN_SEP);
                List<Long> offsets = entry.getValue();
                for (int i = 0; i < offsets.size(); i++) {
                    if (i > 0) sb.append(FILE_OFFSET_SEP);
                    sb.append(offsets.get(i));
                }
                writer.write(sb.toString());
                writer.newLine();
            }
        }
        LOGGER.log(Level.INFO, "GINIndex saved to " + filePath + " with " + postingLists.size() + " tokens");
    }

    /** Load the index from disk. */
    public void loadFromDisk(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        if (!Files.exists(path)) return;

        postingLists.clear();
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] parts = line.split(TOKEN_SEPARATOR, 2);
                if (parts.length < 2) continue;
                String token = parts[0];
                List<Long> offsets = new ArrayList<>();
                for (String off : parts[1].split(OFFSET_SEPARATOR)) {
                    if (!off.isBlank()) offsets.add(Long.parseLong(off.trim()));
                }
                postingLists.put(token, offsets);
            }
        }
        LOGGER.log(Level.INFO, "GINIndex loaded from " + filePath + " with " + postingLists.size() + " tokens");
    }

    // -----------------------------------------------------------------------
    // Tokenization
    // -----------------------------------------------------------------------

    /**
     * Split a value string into lowercase tokens. Strips JSON noise (quotes,
     * braces, colons) so raw JSON document values can be indexed directly.
     */
    static List<String> tokenize(String value) {
        if (value == null || value.isBlank()) return Collections.emptyList();
        // Strip common JSON/punctuation characters, lowercase, split on whitespace
        String cleaned = value.replaceAll("[{}\":\\[\\]]", " ");
        String[] parts  = cleaned.split("\\s+|,");
        List<String> tokens = new ArrayList<>();
        for (String p : parts) {
            String t = p.trim().toLowerCase();
            if (!t.isBlank()) tokens.add(t);
        }
        return tokens;
    }
}
