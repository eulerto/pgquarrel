CREATE OR REPLACE FUNCTION to_event_trigger_2_func()
  RETURNS event_trigger
 LANGUAGE plpgsql
  AS $$
BEGIN
	  RAISE EXCEPTION 'command % is disabled', tg_tag;
END;
$$;

CREATE EVENT TRIGGER to_event_trigger_2 ON ddl_command_start
   EXECUTE PROCEDURE to_event_trigger_2_func();

COMMENT ON EVENT TRIGGER to_event_trigger_2 IS 'this is comment for to_event_trigger_2';
