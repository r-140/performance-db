package com.iu.worker;

public enum ErrorCode {
    IOEXCEPTION("DB-401", "IOException has been thrown"),
    INDEXEXIST("DB-402", "Index with the type {indexType} already Exist"),
    INDEXDOESNOTEXIST("DB-403", "Index with the type {indexType} does not Exist"),
    UNEXPECTEDINDEXTYPE("DB-404", "Unexpected index type"),
    DOCUMENTNOTFOUND("DB-405", "Document with id {id} not found");

    private final String errorCode;
    private final String errorMessage;

    ErrorCode(String errorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
