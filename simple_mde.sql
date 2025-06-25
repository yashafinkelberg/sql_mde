-- Устанавливаем параметры для эксперимента
@set alpha = 0.05 -- Уровень значимости (для 95% доверительного интервала)
@set date_ = date('2024-09-01') -- Дата анализа
@set n_buckets = 50 -- Количество бакетов (группировка пользователей)
@set n_test = 10  -- Количество итераций бакетирования (для устойчивости оценки)
 
WITH  t_inverse_CDF AS ( -- создаем значения для t-распределения, при использовании менее 200 бакетов
    SELECT
        MAX(t_value) AS t_value
    FROM (
        SELECT
            *,
            LEAD(degrees_of_freedom) OVER (PARTITION BY p ORDER BY degrees_of_freedom) AS next_degrees_of_freedom
        FROM (
            VALUES
                (0.025, 1, 12.706204736432102),
                (0.025, 2, 4.302652729696144),
                (0.025, 3, 3.182446305284264),
                (0.025, 4, 2.7764451051977996),
                (0.025, 5, 2.570581835636315),
                (0.025, 6, 2.4469118511449692),
                (0.025, 7, 2.3646242515927844),
                (0.025, 8, 2.3060041352041662),
                (0.025, 9, 2.2621571628540997),
                (0.025, 10, 2.2281388519649385),
                (0.025, 15, 2.131449545559323),
                (0.025, 20, 2.085963447265837),
                (0.025, 25, 2.0595385527532946),
                (0.025, 30, 2.042272456301238),
                (0.025, 40, 2.0210753903062737),
                (0.025, 50, 2.008559112100761),
                (0.025, 60, 2.0002978220142604),
                (0.025, 100, 1.983971518449634),
                (0.025, 1000, 1.9623390808264078),
                (0.05, 1, 6.313751514800938),
                (0.05, 2, 2.919985580355518),
                (0.05, 3, 2.3533634348018273),
                (0.05, 4, 2.13184678632665),
                (0.05, 5, 2.0150483733330233),
                (0.05, 6, 1.9431802805153024),
                (0.05, 7, 1.8945786050613054),
                (0.05, 8, 1.8595480375228428),
                (0.05, 9, 1.8331129326536337),
                (0.05, 10, 1.8124611228107341),
                (0.05, 15, 1.7530503556925552),
                (0.05, 20, 1.7247182429207863),
                (0.05, 25, 1.7081407612518988),
                (0.05, 30, 1.6972608865939578),
                (0.05, 40, 1.6838510133356528),
                (0.05, 50, 1.6759050251630976),
                (0.05, 60, 1.6706488649046367),
                (0.05, 100, 1.6602343260657506),
                (0.05, 1000, 1.6463788172854645),
                (0.1, 1, 3.077683537207806),
                (0.1, 2, 1.8856180831641502),
                (0.1, 3, 1.6377443536962093),
                (0.1, 4, 1.5332062740589427),
                (0.1, 5, 1.4758840488558214),
                (0.1, 6, 1.4397557472577691),
                (0.1, 7, 1.4149239276488585),
                (0.1, 8, 1.3968153097434188),
                (0.1, 9, 1.3830287383964925),
                (0.1, 10, 1.3721836411102861),
                (0.1, 15, 1.3406056078504547),
                (0.1, 20, 1.3253407069850462),
                (0.1, 25, 1.3163450726738701),
                (0.1, 30, 1.3104150253913958),
                (0.1, 40, 1.3030770526071949),
                (0.1, 50, 1.2987136941948099),
                (0.1, 60, 1.295821093498131),
                (0.1, 100, 1.2900747613398766),
                (0.1, 1000, 1.2823987214609245)
        ) AS t (p, degrees_of_freedom, t_value)
        WHERE p = :alpha/2.0000000
    )
    WHERE         :n_buckets - 1 >= degrees_of_freedom         AND
        (:n_buckets - 1 < next_degrees_of_freedom OR next_degrees_of_freedom IS NULL)
)  
, bucketed_data as ( -- разбиваем данные по бакетам
    SELECT
        _id,
        HASH(CAST(user_id AS varchar) || CAST(POWER(17 + _id, 2) AS varchar)) % :n_buckets AS bucket, -- Разбиение на :n_buckets бакетов, внутри каждой итерации различно
        SUM(num) AS num, -- Выбираем целевую метрику для расчета,
        SUM(den) AS den
    FROM (
        SELECT
            _id,
            user_id AS user_id,
            {метрика} AS num-- Замените {метрика} на нужное поле
            {знаменатель} AS den -- Замените {знаменатель} на нужное поле. В случае не ratio metric исползьуйте {знаменатель} = 1
        FROM {витрина} -- Замените {витрина} на нужную таблицу с данными
        CROSS JOIN (SELECT natural_number AS _id FROM dict.natural_number WHERE num <= :n_test) AS t -- Создаем 10 разных разбиений
    ) t1
    WHERE 1=1
    GROUP BY 1,2,3
)
, aggregated_data AS (
    -- Агрегируем данные по бакетам
    SELECT
        _id,
        bucket,
        SUM(num) as num, SUM(den) as den,
        SUM(num) / :n_buckets as num_mean,
        SUM(den) / :n_buckets as den_mean,
        (SUM(num * num) - SUM(num) * SUM(num) / :n_buckets) / (:n_buckets - 1) as num_var,
        (SUM(den * den) - SUM(den) * SUM(den) / :n_buckets) / (:n_buckets - 1) as den_var,
        (SUM(num * den) - SUM(num) * SUM(den) / :n_buckets) / (:n_buckets - 1) as covar
    FROM bucketed_data
    GROUP BY 1,2
)
, prefinal AS (
    SELECT
        AVG(num) AS num,
        AVG(den) AS den,
        AVG(cast(num_mean as double) / den_mean) as mean_ratio,
        AVG(
              1                / pow(den_mean, 2) * num_var
            - 2 * num_mean     / pow(den_mean, 3) * covar
            + pow(num_mean, 2) / pow(den_mean, 4) * den_var
        ) as var_ration -- delta method
        --
        AVG(num_mean) as mean_simple,
        AVG(num_var) as var_simple
    FROM aggregated_data
)
SELECT
    (SELECT * FROM t_inverse_CDF) * sqrt(cast(2 * var as double) / :n_buckets) / mean as MDE
FROM prefinal
