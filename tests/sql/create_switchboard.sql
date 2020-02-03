create schema if not exists switchboard;
drop table if exists switchboard.switchboard_history;
create table switchboard.switchboard_history (
	id serial not null, 
	project_name varchar(32), 
	incoming_path text, 
	outgoing_path text, 
	file_date timestamp without time zone, 
	file_md5 varchar(32), 
	file_size integer, 
	file_path_extract varchar(32), 
	created_date timestamp without time zone, 
	claimed boolean default false, 
	constraint switchboard_history_pkey primary key (id), 
	constraint switchboard_history_outgoing_path_key unique (outgoing_path)
);

INSERT INTO switchboard.switchboard_history(
	  project_name, incoming_path, outgoing_path, file_date, file_md5, file_size, file_path_extract, created_date, claimed)
	VALUES (  'switch_board_test', 'test.csv', '/workspace/tests/sample_data/test.csv', NULL, NULL, NULL, NULL, NULL, False),
	(  'switch_board_test', 'test.csv', '/workspace/tests/sample_data/test_tab.csv', NULL, NULL, NULL, NULL, NULL, False);