CREATE OR REPLACE FUNCTION from_event_trigger_1_func()
  RETURNS event_trigger
 LANGUAGE plpgsql
  AS $$
BEGIN
	  RAISE EXCEPTION 'command % is disabled', tg_tag;
END;
$$;

CREATE EVENT TRIGGER from_event_trigger_1 ON ddl_command_start
   EXECUTE PROCEDURE from_event_trigger_1_func();

COMMENT ON EVENT TRIGGER from_event_trigger_1 IS 'this is comment for from_event_trigger_1';
