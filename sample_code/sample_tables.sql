

create schema if not exists test;

  create table test.test_custom_field
   (	loan_id double precision,
	custom_field_id double precision,
	custom_field_num decimal(12,4),
	custom_field_dt timestamp(0)
   )
  ;

  create table test.test_loan_investor
   (	loan_id decimal(38,0),
	investor_id decimal(38,0)
   )
  ;


  create table test.test_state
   (	program_id decimal(38,0),
	state_cd varchar(4)
   )
  ;


  create table test.test_review_item_type
   (	type_id double precision,
	review_item_id double precision
   )
  ;


  create table test.test_user_client_access
   (	user_id decimal(38,0),
	client_id decimal(38,0)
   )
  ;

  create table test.test_product
   (	user_id decimal(38,0),
	product_id decimal(38,0),
	accepted_time timestamp(0),
	accepted varchar(1),
	version_number decimal(4,2)
   )
  ;

  create table test.test_zip_code
   (	zipcode_lcd varchar(30),
	county_fips_lcd varchar(30),
	state_cd varchar(4),
	city_name varchar(30),
	city_abbr varchar(30),
	preferred_cd varchar(4),
	record_id decimal(38,0)
   )
  ;


  create table test.test_city
   (	city_name varchar(30),
	state_cd varchar(4),
	zipcode_lcd varchar(30),
	area_cd varchar(4),
	county_fips_lcd varchar(30),
	county_name varchar(30),
	preferred_cd varchar(4),
	time_zone_lcd varchar(30),
	dst_ind varchar(1),
	latitude varchar(30),
	longitude varchar(30),
	city_abbr varchar(30),
	market_area_cd varchar(4),
	zipcode_type_cd varchar(4),
	record_id decimal(38,0)
   )
  ;

  create table test.test_version
   (	installed_rank decimal(38,0),
	version varchar(50),
	description varchar(200),
	type varchar(20),
	script varchar(1000),
	checksum decimal(38,0),
	installed_by varchar(100),
	installed_on timestamp (6) default current_timestamp,
	execution_time decimal(38,0),
	success smallint
   )
  ;