WITH property_data AS (
    SELECT
        p.property_type AS property_type,
        p.room_type AS room_type,
        p.accommodates AS accommodates,
        TO_CHAR(l.scraped_date, 'YYYY/MM') AS record_month,
        ROUND(
            SUM(CASE WHEN p.is_available = true THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(p.is_available), 0) * 100, 2
        ) AS occupancy_rate,
        MIN(CASE WHEN p.is_available = true THEN p.price END) AS min_price,
        MAX(CASE WHEN p.is_available = true THEN p.price END) AS max_price,
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY CASE WHEN p.is_available = true THEN p.price END
            )::NUMERIC,
            2
        ) AS median_price,
        ROUND(AVG(CASE WHEN p.is_available = true THEN p.price END), 2) AS avg_price,
        COUNT(DISTINCT host_id) AS unique_hosts,
        ROUND(
            SUM(CASE WHEN h.is_superhost = true THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(h.is_superhost), 0) * 100, 2
        ) AS superhost_rate,
        ROUND(AVG(CASE WHEN p.is_available = true THEN r.review_scores_rating END), 2) AS avg_review_score,
        ROUND(
            (LEAD(
                SUM(CASE WHEN p.is_available = true THEN 1 ELSE 0 END)
                OVER (
                    PARTITION BY p.property_type 
                    ORDER BY TO_CHAR(l.scraped_date, 'YYYY/MM')
                )
            ) - SUM(CASE WHEN p.is_available = true THEN 1 ELSE 0 END))::NUMERIC 
            / NULLIF(SUM(CASE WHEN p.is_available = true THEN 1 ELSE 0 END), 0)::NUMERIC * 100,
            2
        ) AS occupancy_change,
        ROUND(
            (LEAD(
                SUM(CASE WHEN p.is_available = false THEN 1 ELSE 0 END) 
                OVER (
                    PARTITION BY p.property_type 
                    ORDER BY TO_CHAR(l.scraped_date, 'YYYY/MM')
                )
            ) - SUM(CASE WHEN p.is_available = false THEN 1 ELSE 0 END))::NUMERIC 
            / NULLIF(SUM(CASE WHEN p.is_available = true THEN 1 ELSE 0 END), 0)::NUMERIC * 100,
            2
        ) AS inactive_change,
        SUM(CASE WHEN p.is_available = true THEN 30 - p.availability_30 ELSE 0 END) AS total_stays,
        ROUND(AVG(CASE WHEN p.is_available = true THEN (30 - p.availability_30) * p.price ELSE 0 END), 2) AS avg_estimated_revenue
    FROM
        {{ ref('dim_property') }} p
    JOIN
        {{ ref('dim_listing') }} l ON p.id = l.id
    JOIN
        {{ ref('dim_host') }} h ON p.id = h.id
    JOIN
        {{ ref('fact_review') }} r ON p.id = r.id
    GROUP BY
        p.property_type,
        p.room_type,
        p.accommodates,
        record_month
    ORDER BY
        p.property_type,
        p.room_type,
        p.accommodates,
        TO_CHAR(l.scraped_date, 'YYYY/MM')
)

SELECT * FROM property_data;
