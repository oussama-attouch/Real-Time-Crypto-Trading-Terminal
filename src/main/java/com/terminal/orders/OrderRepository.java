package com.terminal.orders;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

/**
 * Spring Data JPA repository for {@link Order}.
 *
 * <p>Derived query methods only — no hand-written SQL, so a typo becomes a startup
 * failure rather than a runtime one. {@code findAllByOrderByCreatedAtDesc()} backs the
 * "recent orders" view that Phase 9 will expose.
 */
@Repository
public interface OrderRepository extends JpaRepository<Order, UUID> {

    List<Order> findTop50ByOrderByCreatedAtDesc();

    List<Order> findBySymbolOrderByCreatedAtDesc(String symbol);
}
