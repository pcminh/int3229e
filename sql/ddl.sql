-- public.d_camera definition

-- Drop table

-- DROP TABLE d_camera;

CREATE TABLE d_camera (
	camera_id varchar NOT NULL,
	"type" varchar NULL,
	red_light_intersection varchar NULL,
	address varchar NULL,
	street_key int2 NULL,
	latitude float8 NULL,
	longitude float8 NULL,
	CONSTRAINT d_camera_pkey PRIMARY KEY (camera_id)
);


-- public.d_date definition

-- Drop table

-- DROP TABLE d_date;

CREATE TABLE d_date (
	date_key int4 NOT NULL,
	date_alternate_key varchar(12) NULL,
	calendar_year int2 NULL,
	calendar_month varchar(10) NULL,
	month_of_year int2 NULL,
	calendar_day varchar(10) NULL,
	day_of_week_sun int2 NULL,
	day_of_week_mon int2 NULL,
	is_week_day bpchar(1) NULL,
	day_of_month int2 NULL,
	is_last_day_of_month bpchar(1) NULL,
	day_of_year int2 NULL,
	week_of_year_iso int2 NULL,
	quater_of_year int2 NULL,
	CONSTRAINT d_date_pkey PRIMARY KEY (date_key)
);


-- public.d_hour definition

-- Drop table

-- DROP TABLE d_hour;

CREATE TABLE d_hour (
	hour_key int2 NOT NULL,
	meridiem_hour int2 NULL,
	meridiem_suffix varchar(2) NULL,
	day_night varchar(6) NULL,
	CONSTRAINT d_hour_pkey PRIMARY KEY (hour_key)
);


-- public.d_segment definition

-- Drop table

-- DROP TABLE d_segment;

CREATE TABLE d_segment (
	segment_id int2 NOT NULL,
	street_key int2 NULL,
	street_heading varchar NULL,
	street varchar NULL,
	street_name varchar NULL,
	direction varchar NULL,
	from_street varchar NULL,
	to_street varchar NULL,
	length float8 NULL,
	start_latitude float8 NULL,
	start_longitude float8 NULL,
	end_latitude float8 NULL,
	end_longitude float8 NULL,
	CONSTRAINT d_segment_pkey PRIMARY KEY (segment_id)
);


-- public.d_street definition

-- Drop table

-- DROP TABLE d_street;

CREATE TABLE d_street (
	street_key int2 NOT NULL,
	direction varchar NULL,
	street varchar NULL,
	suffix varchar NULL,
	suffix_direction varchar NULL,
	full_street_name varchar NULL,
	min_address int2 NULL,
	max_address int2 NULL,
	CONSTRAINT d_street_pkey PRIMARY KEY (street_key)
);


-- public.f_congestion definition

-- Drop table

-- DROP TABLE f_congestion;

CREATE TABLE f_congestion (
	record_id varchar NULL,
	segment_id int2 NULL,
	"timestamp" timestamp NULL,
	gmtts_str varchar NULL,
	speed int2 NULL,
	bus_count int2 NULL,
	message_count int2 NULL,
	date_key int4 NULL,
	hour_key int4 NULL
);


-- public.f_crash definition

-- Drop table

-- DROP TABLE f_crash;

CREATE TABLE f_crash (
	crash_record_id varchar NOT NULL,
	report_number varchar NULL,
	crash_timestamp timestamp NULL,
	crash_date_key int4 NULL,
	crash_hour_key int2 NULL,
	posted_speed_limit int2 NULL,
	traffic_control_device varchar NULL,
	device_condition varchar NULL,
	weather_condition varchar NULL,
	lighting_condition varchar NULL,
	roadway_surface_cond varchar NULL,
	road_defect varchar NULL,
	first_crash_type varchar NULL,
	trafficway_type varchar NULL,
	lane_cnt int4 NULL,
	report_type varchar NULL,
	crash_type varchar NULL,
	damage varchar NULL,
	street_no varchar NULL,
	street_key int2 NULL,
	num_units int2 NULL,
	most_severe_injury varchar NULL,
	injuries_total int2 NULL,
	injuries_fatal int2 NULL,
	injuries_incapacitating int2 NULL,
	injuries_non_incapacitating int2 NULL,
	injuries_report_not_evident int2 NULL,
	injuries_no_indication int2 NULL,
	injuries_unknown int2 NULL,
	latitude float8 NULL,
	longitude float8 NULL,
	CONSTRAINT f_crash_pkey PRIMARY KEY (crash_record_id)
);


-- public.f_violation definition

-- Drop table

-- DROP TABLE f_violation;

CREATE TABLE f_violation (
	camera_id varchar NULL,
	violation_date_key int4 NULL,
	violation_type varchar NULL,
	violations int2 NULL
);