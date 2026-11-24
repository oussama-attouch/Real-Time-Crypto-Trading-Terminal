package com.terminal.orders;

import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps persistence failures onto honest HTTP statuses.
 *
 * <p>The distinction matters: a client that gets {@code 400} for a database outage would
 * "fix" its payload forever, and a client that gets {@code 500} would not know it may
 * safely retry. {@link HttpStatus#SERVICE_UNAVAILABLE} plus {@code Retry-After} says
 * exactly the right thing — the request was fine, the dependency was not, try again.
 *
 * <p>Connection failures reach here in several shapes depending on where they happen:
 * opening the transaction, borrowing from Hikari, or flushing at commit. Hence the
 * cause-chain walk rather than a single {@code instanceof}.
 */
@Slf4j
@RestControllerAdvice
public class OrderExceptionHandler {

    /** Thrown when the transaction manager cannot obtain a connection at all. */
    @ExceptionHandler(CannotCreateTransactionException.class)
    public ResponseEntity<ProblemDetail> databaseUnavailable(CannotCreateTransactionException ex) {
        return unavailable(ex);
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ProblemDetail> dataAccessFailure(DataAccessException ex) {
        if (isConnectivityFailure(ex)) {
            return unavailable(ex);
        }
        log.error("Order persistence failed", ex);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.INTERNAL_SERVER_ERROR, "Unexpected persistence failure");
        problem.setTitle("Internal error");
        problem.setType(URI.create("about:blank"));
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(problem);
    }

    /** Bean-validation failures: {@code price <= 0}, unknown side, missing symbol. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ProblemDetail> invalidRequest(MethodArgumentNotValidException ex) {
        Map<String, String> fieldErrors = new LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(error -> fieldErrors.putIfAbsent(error.getField(), error.getDefaultMessage()));

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Order rejected");
        problem.setTitle("Validation failed");
        problem.setType(URI.create("about:blank"));
        problem.setProperty("errors", fieldErrors);
        return ResponseEntity.badRequest().body(problem);
    }

    private ResponseEntity<ProblemDetail> unavailable(Exception ex) {
        // Logged at WARN, not ERROR: an unavailable dependency is expected and already
        // surfaced to the caller; ERROR here would page someone for a Postgres restart.
        log.warn("Order rejected: database unavailable ({})", rootCause(ex));

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.SERVICE_UNAVAILABLE, "Order store unavailable, retry shortly");
        problem.setTitle("Service unavailable");
        problem.setType(URI.create("about:blank"));
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", "5")
                .body(problem);
    }

    private static boolean isConnectivityFailure(Throwable throwable) {
        if (throwable instanceof DataAccessResourceFailureException) {
            return true;
        }
        Throwable current = throwable;
        while (current != null && current != current.getCause()) {
            if (current instanceof SQLTransientConnectionException
                    || current instanceof SQLNonTransientConnectionException
                    || current instanceof DataAccessResourceFailureException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static String rootCause(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + (root.getMessage() == null ? "" : ": " + root.getMessage());
    }
}
