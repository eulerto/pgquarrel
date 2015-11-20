BEGIN;
ALTER TABLE ONLY customers ADD COLUMN apelido varchar(50);
ALTER TABLE ONLY customers DROP COLUMN address2;
UPDATE customers SET state = 'TO' WHERE state IS NULL;
ALTER TABLE ONLY customers ALTER COLUMN state SET NOT NULL;
ALTER TABLE ONLY customers ALTER COLUMN region SET DATA TYPE integer;

ALTER TABLE ONLY orderlines ALTER COLUMN quantity SET DATA TYPE integer;
END;
