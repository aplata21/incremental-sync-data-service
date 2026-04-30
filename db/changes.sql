-- =============================================================================
-- Caseware incremental-sync prototype: deterministic incremental changes
--
-- Applied between the first and second /ingest run to validate incrementality.
--
-- Change anchor: 2026-04-15T12:00:00Z
--   - Strictly later than every timestamp produced by db/init.sql.
--   - All 17 mutations (5 case updates, 2 customer inserts, 10 case inserts)
--     share this exact updated_at. That is intentional: the second ingest
--     pass must order them deterministically by primary key, and the
--     composite watermark must consume them as a single tie cluster.
--
-- Determinism guarantees:
--   - case_ids 1, 50, 100, 150, 200 chosen as fixed update targets.
--   - new customer_ids will be 31 and 32 (next IDENTITY values after seed = 30).
--   - new case_ids will be 201..210 (next IDENTITY values after seed = 200).
--   - foreign-key spread for new cases is hardcoded.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1) Update exactly 5 existing cases: change status and updated_at.
-- -----------------------------------------------------------------------------
UPDATE cases
   SET status = 'escalated',
       updated_at = TIMESTAMPTZ '2026-04-15T12:00:00Z'
 WHERE case_id = 1;

UPDATE cases
   SET status = 'closed',
       updated_at = TIMESTAMPTZ '2026-04-15T12:00:00Z'
 WHERE case_id = 50;

UPDATE cases
   SET status = 'resolved',
       updated_at = TIMESTAMPTZ '2026-04-15T12:00:00Z'
 WHERE case_id = 100;

UPDATE cases
   SET status = 'in_review',
       updated_at = TIMESTAMPTZ '2026-04-15T12:00:00Z'
 WHERE case_id = 150;

UPDATE cases
   SET status = 'pending',
       updated_at = TIMESTAMPTZ '2026-04-15T12:00:00Z'
 WHERE case_id = 200;

-- -----------------------------------------------------------------------------
-- 2) Insert exactly 2 new customers (-> customer_id 31 and 32).
-- -----------------------------------------------------------------------------
INSERT INTO customers (name, email, country, updated_at) VALUES
    ('Customer 031', 'customer031@example.com', 'US',
     TIMESTAMPTZ '2026-04-15T12:00:00Z'),
    ('Customer 032', 'customer032@example.com', 'CA',
     TIMESTAMPTZ '2026-04-15T12:00:00Z');

-- -----------------------------------------------------------------------------
-- 3) Insert exactly 10 new cases (-> case_id 201..210).
--    Foreign keys deliberately mix existing customers and the 2 new ones.
-- -----------------------------------------------------------------------------
INSERT INTO cases (customer_id, title, description, status, updated_at) VALUES
    ( 1, 'Case 0201 - Billing review',
         'Followup billing reconciliation for customer 1.',
         'open',      TIMESTAMPTZ '2026-04-15T12:00:00Z'),
    (31, 'Case 0202 - Onboarding review',
         'Onboarding workflow for new customer 31.',
         'open',      TIMESTAMPTZ '2026-04-15T12:00:00Z'),
    ( 5, 'Case 0203 - AML review',
         'AML compliance check for customer 5.',
         'in_review', TIMESTAMPTZ '2026-04-15T12:00:00Z'),
    (32, 'Case 0204 - Onboarding review',
         'Onboarding workflow for new customer 32.',
         'open',      TIMESTAMPTZ '2026-04-15T12:00:00Z'),
    (10, 'Case 0205 - Fraud review',
         'Fraud check for customer 10.',
         'pending',   TIMESTAMPTZ '2026-04-15T12:00:00Z'),
    (31, 'Case 0206 - Compliance review',
         'Compliance review for new customer 31.',
         'open',      TIMESTAMPTZ '2026-04-15T12:00:00Z'),
    (15, 'Case 0207 - Payments review',
         'Payments dispute for customer 15.',
         'in_review', TIMESTAMPTZ '2026-04-15T12:00:00Z'),
    (32, 'Case 0208 - Audit review',
         'Audit follow-up for new customer 32.',
         'open',      TIMESTAMPTZ '2026-04-15T12:00:00Z'),
    (20, 'Case 0209 - Reconciliation review',
         'Reconciliation issue for customer 20.',
         'pending',   TIMESTAMPTZ '2026-04-15T12:00:00Z'),
    (25, 'Case 0210 - Billing review',
         'Billing dispute for customer 25.',
         'escalated', TIMESTAMPTZ '2026-04-15T12:00:00Z');

COMMIT;
