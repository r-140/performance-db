package com.iu.worker.error;

import org.json.JSONObject;

/**
 * Sealed interface for DB errors (JEP 409, Java 17 — finalized in Java 21).
 *
 * A sealed type restricts which classes can implement/extend it, making it
 * exhaustively matchable in a switch expression. This replaces the original
 * ErrorCode enum with a richer hierarchy where each error type can carry
 * its own contextual data rather than just a message template.
 *
 * Pattern-matching switch (JEP 441, Java 21) works exhaustively over this:
 *
 *   String msg = switch (error) {
 *       case IoError e      -> "IO error: " + e.cause().getMessage();
 *       case IndexExists e  -> "Index already exists: " + e.indexType();
 *       case IndexMissing e -> "Index not found: " + e.indexType();
 *       case BadIndexType e -> "Unknown index type: " + e.indexType();
 *       case DocNotFound e  -> "Document not found: id=" + e.id();
 *   };
 *
 * The compiler enforces that all subtypes are listed — no default needed,
 * and adding a new subtype without updating every switch is a compile error.
 */
public sealed interface DbError
        permits DbError.IoError,
                DbError.IndexExists,
                DbError.IndexMissing,
                DbError.BadIndexType,
                DbError.DocNotFound {

    /** HTTP-style error code for wire protocol. */
    String code();

    /** Human-readable message for the client. */
    String message();

    /** Serialize to the existing JSON wire format. */
    default String toJson() {
        return new JSONObject()
                .put("code",    code())
                .put("message", message())
                .toString();
    }

    // ------------------------------------------------------------------
    // Concrete subtypes — all records for conciseness
    // ------------------------------------------------------------------

    record IoError(Exception cause) implements DbError {
        @Override public String code()    { return "DB-401"; }
        @Override public String message() { return "IO error: " + cause.getMessage(); }
    }

    record IndexExists(String indexType) implements DbError {
        @Override public String code()    { return "DB-402"; }
        @Override public String message() { return "Index '" + indexType + "' already exists"; }
    }

    record IndexMissing(String indexType) implements DbError {
        @Override public String code()    { return "DB-403"; }
        @Override public String message() { return "Index '" + indexType + "' does not exist"; }
    }

    record BadIndexType(String indexType) implements DbError {
        @Override public String code()    { return "DB-404"; }
        @Override public String message() { return "Unknown index type: '" + indexType + "'"; }
    }

    record DocNotFound(int id) implements DbError {
        @Override public String code()    { return "DB-405"; }
        @Override public String message() { return "Document with id=" + id + " not found"; }
    }
}
