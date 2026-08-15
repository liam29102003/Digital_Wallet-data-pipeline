

with pipeline as (

    select
        run_id,
        table_name,
        pipeline_name,
        status as pipeline_status,
        started_at,
        ended_at,
        duration_seconds,
        rows_processed as pipeline_rows_processed,
        error_message as pipeline_error_message
    from {{ source('observability', 'pipeline_run_log') }}
    where stage = 'ingestion'
      and table_name is not null

),

reconciliation as (

    select
        run_id,
        table_name,
        source_system,
        extracted_count,
        written_count,
        quarantined_count as reconciliation_quarantined_count,
        unexplained_gap,
        matched as reconciliation_matched,
        checked_at as reconciled_at
    from {{ source('observability', 'reconciliation_log') }}

),

quarantine_summary as (

    select
        run_id,
        table_name,
        count(*) as quarantined_row_count,
        max(quarantined_at) as last_quarantined_at
    from {{ source('observability', 'quarantine_records') }}
    group by run_id, table_name

),

final as (

    select
        coalesce(p.run_id, r.run_id, q.run_id) as run_id,
        coalesce(p.table_name, r.table_name, q.table_name) as table_name,

        -- pipeline execution
        p.pipeline_name,
        p.pipeline_status,
        p.started_at,
        p.ended_at,
        p.duration_seconds,
        p.pipeline_error_message,

        -- reconciliation
        r.source_system,
        r.extracted_count,
        r.written_count,
        r.unexplained_gap,
        r.reconciliation_matched,
        r.reconciled_at,
        coalesce(q.quarantined_row_count, r.reconciliation_quarantined_count, 0) as quarantined_row_count,
        q.last_quarantined_at,
        case
            when p.pipeline_status = 'failed' then 'pipeline_failed'
            when r.reconciliation_matched = false then 'reconciliation_mismatch'
            when coalesce(q.quarantined_row_count, 0) > 0 then 'quarantined_rows_present'
            when p.pipeline_status = 'success' and r.reconciliation_matched = true then 'clean'
            else 'incomplete_data'
        end as batch_health_status

    from pipeline p
    full outer join reconciliation r
        on p.run_id = r.run_id and p.table_name = r.table_name
    full outer join quarantine_summary q
        on coalesce(p.run_id, r.run_id) = q.run_id
        and coalesce(p.table_name, r.table_name) = q.table_name

)

select * from final