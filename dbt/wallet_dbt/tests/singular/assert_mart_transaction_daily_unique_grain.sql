-- Singular test: asserts the grain of mart_transaction_daily is truly
-- unique on (transaction_date, currency, transaction_type, status).
--
-- dbt singular tests pass when the query returns ZERO rows. This finds
-- any grain combination that appears more than once — which should be
-- impossible given the GROUP BY in the model, but this test guards
-- against a future edit to the model accidentally breaking that grain
-- (e.g. someone adds a column to the SELECT without adding it to the
-- GROUP BY, silently fanning out rows).

select
    transaction_date,
    currency,
    transaction_type,
    status,
    count(*) as row_count
from {{ ref('mart_transaction_daily') }}
group by
    transaction_date,
    currency,
    transaction_type,
    status
having count(*) > 1