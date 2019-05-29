CREATE OR REPLACE FUNCTION test.inc(val integer)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
BEGIN
RETURN val + 1;
END; $function$
;
GRANT ALL ON FUNCTION test.inc TO operational_dba;