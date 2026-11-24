package com.terminal.orders;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

/**
 * Write side of the terminal.
 *
 * <p>{@code POST /api/orders} is the only mutating endpoint. The Vite dev proxy already
 * forwards {@code /api/**} to this service, so the browser calls a same-origin path and
 * there is no CORS configuration to get wrong.
 *
 * <p>Returns {@code 201 Created} with a {@code Location} header pointing at the order —
 * the resource a future {@code GET /api/orders/{id}} will serve. The body carries the id and
 * status the UI needs, so the client does not have to follow the header to confirm success.
 *
 * <p>Validation failures ({@code price <= 0}, bad side, missing symbol) are rejected by
 * {@code @Valid} before this method body runs, and {@link OrderExceptionHandler} renders
 * them as {@code 400} with per-field messages.
 */
@Slf4j
@RestController
@RequestMapping(path = "/api/orders", produces = MediaType.APPLICATION_JSON_VALUE)
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<PlaceOrderResponse> placeOrder(@Valid @RequestBody PlaceOrderRequest request) {
        PlaceOrderResponse response = orderService.placeOrder(request);
        return ResponseEntity
                .created(URI.create("/api/orders/" + response.orderId()))
                .body(response);
    }
}
