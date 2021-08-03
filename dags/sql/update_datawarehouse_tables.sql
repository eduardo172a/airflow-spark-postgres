INSERT INTO origin_group (origin_x,origin_y,region, count)
SELECT origin_x,origin_y,region, count(*) FROM trip_transformed GROUP BY origin_x, origin_y, region ORDER BY count DESC;

INSERT INTO destination_group (dest_x,dest_y,region, count)
SELECT dest_x,dest_y,region, count(*) FROM trip_transformed GROUP BY dest_x, dest_y, region ORDER BY count DESC;

INSERT INTO weekly_average (week,year,region,weekly_average)
select week,year,region ,cast(CAST(count(*) AS float) / CAST(7 AS float) as decimal(4,2)) from trip_transformed group by region,year,week order by region,year,week;

INSERT INTO time_of_day (hour_bucket,region,count)
select hour_bucket, region, count(*) from trip_transformed group by hour_bucket,region order by region, count desc;