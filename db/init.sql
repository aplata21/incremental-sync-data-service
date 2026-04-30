-- =============================================================================
-- Caseware incremental-sync prototype: schema + deterministic seed
--
-- Conventions (purely deterministic; no random()):
--   Anchor:                       2026-03-01T00:00:00Z (UTC)
--   customers.updated_at(i):      anchor + (i - 1) * INTERVAL '1 day'         i in 1..30
--                                 -> 30 unique timestamps spanning 30 days.
--   cases.updated_at(i):          anchor + ((i - 1) % 100) * (INTERVAL '30 days' / 100)
--                                 -> 100 unique timestamps, each shared by
--                                    exactly two case rows. The intentional
--                                    (updated_at, case_id) ties are what make
--                                    the composite watermark worth proving.
--   country, status, keyword:     cycled deterministically by row index.
--
-- Latest seed timestamp (for the spec requirement that db/changes.sql uses a
-- fixed timestamp literal strictly later than every seed timestamp):
--   customers max:  2026-03-30T00:00:00Z   (i = 30)
--   cases max:      2026-03-30T16:48:00Z   (i % 100 = 99, 99 * 7h12m = 712h48m)
-- changes.sql uses 2026-04-15T12:00:00Z, which is well after both.
-- =============================================================================

CREATE TABLE customers (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT NOT NULL UNIQUE,
    country     TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE cases (
    case_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id),
    title       TEXT NOT NULL,
    description TEXT NOT NULL,
    status      TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_customers_updated_at ON customers (updated_at);
CREATE INDEX idx_cases_updated_at     ON cases     (updated_at);
CREATE INDEX idx_cases_customer_id    ON cases     (customer_id);

-- -----------------------------------------------------------------------------
-- Seed: customers (30 rows, 30-day window, no ties among customers)
-- -----------------------------------------------------------------------------
INSERT INTO customers (name, email, country, updated_at)
SELECT
    'Customer ' || lpad(i::text, 3, '0'),
    'customer' || lpad(i::text, 3, '0') || '@example.com',
    (ARRAY['US', 'CA', 'GB', 'DE', 'AU'])[((i - 1) % 5) + 1],
    TIMESTAMPTZ '2026-03-01T00:00:00Z' + ((i - 1) * INTERVAL '1 day')
FROM generate_series(1, 30) AS s(i);

-- -----------------------------------------------------------------------------
-- Seed: cases (200 rows, 30-day window, 100 unique updated_at values)
--
-- Each timestamp slot is occupied by exactly two case rows (i and i+100),
-- which means consecutive case_ids 1 and 101, 2 and 102, ..., 100 and 200
-- share the same updated_at. This guarantees the composite watermark
--   (updated_at = ckpt.updated_at AND case_id > ckpt.last_pk)
-- is always exercised on the boundary, regardless of how a partial run
-- happens to land.
-- -----------------------------------------------------------------------------
WITH params AS (
    SELECT
        i,
        (ARRAY[
            'billing', 'audit', 'compliance', 'payments',
            'reconciliation', 'onboarding', 'fraud', 'AML'
        ])[((i - 1) % 8) + 1] AS keyword,
        (ARRAY['open', 'in_review', 'resolved', 'closed', 'pending', 'escalated'])
            [((i - 1) % 6) + 1] AS status,
        (((i - 1) % 30) + 1)::bigint AS customer_id
    FROM generate_series(1, 200) AS s(i)
)
INSERT INTO cases (customer_id, title, description, status, updated_at)
SELECT
    customer_id,
    'Case ' || lpad(i::text, 4, '0') || ' - ' || initcap(keyword) || ' review',
    initcap(keyword)
        || ' workflow item ' || i
        || ' for customer ' || customer_id
        || ': follow up on ' || keyword
        || ' details and supporting documents.',
    status,
    TIMESTAMPTZ '2026-03-01T00:00:00Z'
        + (((i - 1) % 100) * (INTERVAL '30 days' / 100))
FROM params
ORDER BY i;
