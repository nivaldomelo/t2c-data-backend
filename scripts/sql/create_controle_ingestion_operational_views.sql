create schema if not exists controle;

drop view if exists controle.vw_t2c_log_operacional;
drop view if exists controle.vw_t2c_historico_operacional;
drop view if exists controle.vw_t2c_ingestao_operacional;

create or replace view controle.vw_t2c_ingestao_operacional as
with latest_execution as (
    select distinct on (e.pipeline_id)
        e.pipeline_id,
        e.execucao_id,
        e.dag_run_id,
        e.task_id,
        e.started_at,
        e.finished_at,
        e.status,
        e.watermark_before,
        e.watermark_after,
        e.linhas_extraidas,
        e.linhas_gravadas,
        e.linhas_upsert,
        e.erro_tipo,
        e.erro_mensagem,
        e.detalhes_json
    from controle.t2c_execucao_pipeline_mysql_pg e
    order by e.pipeline_id, e.started_at desc, e.execucao_id desc
),
last_success as (
    select
        e.pipeline_id,
        max(coalesce(e.finished_at, e.started_at)) as last_success_at
    from controle.t2c_execucao_pipeline_mysql_pg e
    where upper(e.status) in ('SUCCESS', 'SUCESSO', 'COMPLETED')
    group by e.pipeline_id
),
last_failure as (
    select
        e.pipeline_id,
        max(coalesce(e.finished_at, e.started_at)) as last_failure_at
    from controle.t2c_execucao_pipeline_mysql_pg e
    where upper(e.status) in ('FAILED', 'FALHA', 'ERROR', 'ERRO')
    group by e.pipeline_id
)
select
    p.pipeline_id,
    p.pipeline_name,
    p.dag_id,
    le.task_id as task_name,
    p.source_conn_id as source_connection,
    p.source_database,
    p.source_table,
    p.target_conn_id,
    p.target_schema,
    p.target_table,
    p.tipo_carga as load_type,
    p.pk_columns,
    p.watermark_column,
    p.watermark_expression_sql,
    p.watermark_type,
    coalesce(
        p.watermark_atual_text,
        to_char(p.watermark_atual_ts, 'YYYY-MM-DD"T"HH24:MI:SSOF'),
        p.watermark_atual_bigint::text
    ) as watermark_value,
    p.airflow_schedule,
    p.airflow_owner,
    p.airflow_tags,
    p.is_active,
    p.ultima_execucao_inicio as last_execution_started_at,
    p.ultima_execucao_fim as last_execution_finished_at,
    p.ultima_execucao_status as latest_status,
    p.ultimo_dag_run_id,
    p.ultimo_erro as last_error,
    p.linhas_extraidas_ultima as rows_extracted,
    p.linhas_gravadas_ultima as rows_written,
    p.linhas_upsert_ultima as rows_upserted,
    coalesce(p.linhas_upsert_ultima, p.linhas_gravadas_ultima, p.linhas_extraidas_ultima) as rows_processed,
    le.execucao_id as execution_id,
    le.dag_run_id,
    le.watermark_before,
    le.watermark_after,
    le.erro_tipo,
    le.erro_mensagem,
    le.detalhes_json,
    ls.last_success_at,
    lf.last_failure_at,
    p.created_at,
    coalesce(p.ultima_execucao_fim, p.ultima_execucao_inicio, p.created_at) as updated_at
from controle.t2c_controle_pipeline_mysql_pg p
left join latest_execution le on le.pipeline_id = p.pipeline_id
left join last_success ls on ls.pipeline_id = p.pipeline_id
left join last_failure lf on lf.pipeline_id = p.pipeline_id;


create or replace view controle.vw_t2c_historico_operacional as
with pipeline_success_stats as (
    select
        e.pipeline_id,
        count(*) as window_runs,
        sum(case when upper(e.status) in ('FAILED', 'FALHA', 'ERROR', 'ERRO') then 1 else 0 end) as failed_runs,
        round(
            100.0 * avg(
                case
                    when upper(e.status) in ('SUCCESS', 'SUCESSO', 'COMPLETED') then 1.0
                    else 0.0
                end
            )::numeric,
            1
        ) as success_rate_pct,
        max(
            case
                when upper(e.status) in ('SUCCESS', 'SUCESSO', 'COMPLETED')
                then coalesce(e.finished_at, e.started_at)
            end
        ) as last_success_at
    from controle.t2c_execucao_pipeline_mysql_pg e
    group by e.pipeline_id
)
select
    e.execucao_id as execution_id,
    e.pipeline_id,
    p.pipeline_name,
    e.dag_id,
    e.dag_run_id,
    e.task_id as task_name,
    p.source_conn_id as source_connection,
    p.source_database,
    p.source_table,
    p.target_schema,
    p.target_table,
    p.tipo_carga as load_type,
    e.status,
    e.started_at,
    e.finished_at,
    extract(epoch from (coalesce(e.finished_at, e.started_at) - e.started_at))::bigint as duration_seconds,
    e.watermark_before,
    e.watermark_after,
    e.linhas_extraidas as rows_extracted,
    e.linhas_gravadas as rows_written,
    e.linhas_upsert as rows_upserted,
    coalesce(e.linhas_upsert, e.linhas_gravadas, e.linhas_extraidas) as rows_processed,
    e.erro_tipo,
    e.erro_mensagem as error_message,
    e.detalhes_json,
    coalesce(e.finished_at, e.started_at) as bucket_start_at,
    stats.last_success_at,
    e.finished_at as last_execution_finished_at,
    stats.window_runs,
    coalesce(stats.success_rate_pct, 0) as success_rate_pct,
    coalesce(stats.failed_runs, 0) as failed_runs,
    case
        when upper(e.status) in ('FAILED', 'FALHA', 'ERROR', 'ERRO') and coalesce(stats.failed_runs, 0) >= 2 then true
        else false
    end as recurrent_degradation,
    case
        when stats.last_success_at is null then true
        when stats.last_success_at <= now() - interval '72 hours' then true
        else false
    end as currently_stale
from controle.t2c_execucao_pipeline_mysql_pg e
join controle.t2c_controle_pipeline_mysql_pg p on p.pipeline_id = e.pipeline_id
left join pipeline_success_stats stats on stats.pipeline_id = e.pipeline_id;


create or replace view controle.vw_t2c_log_operacional as
select
    l.log_id,
    l.execucao_id as execution_id,
    e.pipeline_id,
    p.pipeline_name,
    e.dag_id,
    e.dag_run_id,
    e.task_id as task_name,
    p.target_schema,
    p.target_table,
    l.event_ts as occurred_at,
    l.nivel as level,
    l.etapa as step,
    l.mensagem as message,
    l.status,
    l.rows_affected,
    l.watermark_before_text,
    l.watermark_after_text,
    l.detalhes_json,
    l.stacktrace
from controle.t2c_log_pipeline_mysql_pg l
join controle.t2c_execucao_pipeline_mysql_pg e on e.execucao_id = l.execucao_id
join controle.t2c_controle_pipeline_mysql_pg p on p.pipeline_id = e.pipeline_id;
