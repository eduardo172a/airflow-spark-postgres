CREATE TABLE IF NOT EXISTS origin_group(origin_x numeric(4,1), origin_y numeric(4,1),region text,count int8);
CREATE TABLE IF NOT EXISTS destination_group(dest_x numeric(4,1), dest_y numeric(4,1),region text,count int8);
CREATE TABLE IF NOT EXISTS weekly_average(week int8, year int8,region text,weekly_average numeric(4,2));
CREATE TABLE IF NOT EXISTS time_of_day(hour_bucket text, region text, count int8);