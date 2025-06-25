-- Устанавливаем параметры для эксперимента
@set alpha = 0.05 -- Уровень значимости (для 95% доверительного интервала)
@set date_ = date('2024-09-01') -- Дата анализа
@set n_buckets = 50 -- Количество бакетов (группировка пользователей)
@set n_test = 10  -- Количество итераций бакетирования (для устойчивости оценки)
 
WITH bucketed_data as ( -- разбиваем данные по бакетам     select
        _id,
        variant_id,
        HASH(CAST(user_id AS varchar) || CAST(POWER(17 + _id, 2) AS varchar)) % :n_buckets as bucket, -- разбиваем на бакеты, при чем в каждом из разбиений -- делаем по разному за счет _id
        --
        vertical,
        segment_rank,
        event_month,
        --
        SUM(tickets_cnt)*1.0 AS tickets_cnt,
        SUM(rev)*1.0 AS rev,
        COUNT(user_id)*1.0 AS user_cnt
    from    (
                SELECT
                    num as _id,
                    hash(CAST(user_id AS varchar) || CAST(num AS varchar)) % 2 as variant_id,
                    user_id AS user_id,
                    event_month,
                    vertical,
                    segment_rank,
                    MAX(revenue) AS rev,
                    SUM(tickets_cnt) AS tickets_cnt
                FROM {витрина с пользователями}
                    CROSS JOIN (SELECT num FROM dict.natural_number WHERE num <= :n_test) as t -- создаем 10 расчетов
                WHERE event_month = date('2024-09-01')
                GROUP BY 1,2,3,4,5
            ) t1
    where 1=1
    group by 1,2,3,4,5
)
, bucketed_data_w_metric as (
    SELECT
        _id,
        variant_id,
        event_month,
        bucket,
        CAST(
            SUM(share*CR) AS DECIMAL(20,15)
        ) / (date_diff('day', date_trunc('month', event_month), last_day_of_month(event_month)) + 1) AS num -- считаем метрику как она есть
    FROM (
        SELECT
            _id,
            variant_id,
            event_month,
            bucket,
            vertical,
            segment_rank,
            rev / (SUM(rev) OVER (PARTITION BY bucket,_id,event_month)) AS share,
            tickets_cnt * 1.0 / user_cnt  AS CR
        FROM bucketed_data
    ) AS t1
    GROUP BY 1,2,3
)
, aggregated_data AS (
    -- Агрегируем данные по бакетам
    SELECT
        _id,
        variant_id,
        bucket,
        SUM(num) as num,
        SUM(num) / :n_buckets as num_mean,
        (SUM(num * num) - SUM(num) * SUM(num) / :n_buckets) / (:n_buckets - 1) as num_var,
    FROM bucketed_data
    GROUP BY 1,2
)
    ,   final AS (
        select
            _id,
            sum(case when variant_id % 2 = 1 then -num_mean else num_mean end) as st_mean,
            sum(num_var / :nb) as st_var,
            sum(case when variant_id % 2 = 1 then -num_mean else num_mean end)
                / sqrt(sum(num_var / :nb)) as x
        FROM pre_final
        GROUP BY 1
    )
    SELECT * FROM final
