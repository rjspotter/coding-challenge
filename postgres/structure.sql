CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA PUBLIC;

CREATE TABLE public.raw_transactions (
  actor varchar NOT NULL,
  target varchar NOT NULL,
  created_time timestamp NOT NULL
);

ALTER TABLE ONLY public.raw_transactions
  ADD CONSTRAINT raw_transactions_pkey PRIMARY KEY (actor, target, created_time);

CREATE SEQUENCE median_order
  AS bigint
  INCREMENT BY 1 START WITH 1
  MINVALUE 1 NO CYCLE;

CREATE TABLE public.rolling_medians (
  id bigint NOT NULL PRIMARY KEY DEFAULT nextval('median_order'),
  calc_median DECIMAL(5, 2) NOT NULL,
  created_time timestamp NOT NULL
);

CREATE OR REPLACE FUNCTION _final_median (numeric[])
  RETURNS numeric
  AS $$
  SELECT
    AVG(val)
  FROM (
    SELECT
      val
    FROM
      unnest($1) val
    ORDER BY
      1
    LIMIT 2 - MOD(array_upper($1, 1), 2) OFFSET CEIL(array_upper($1, 1) / 2.0) - 1) sub;

$$
LANGUAGE 'sql'
IMMUTABLE;

CREATE AGGREGATE median (numeric) (
  SFUNC = array_append,
  STYPE = numeric[],
  FINALFUNC = _final_median,
  INITCOND = '{}'
);

CREATE OR REPLACE VIEW filtered AS (
  SELECT DISTINCT
    *
  FROM
    raw_transactions
  WHERE
    created_time >= (
      SELECT
        max(created_time)
      FROM
        raw_transactions) - interval '60 seconds');

CREATE OR REPLACE VIEW nodes AS (
  SELECT
    actor AS node,
    created_time
  FROM
    filtered
  UNION
  SELECT
    target AS node,
    created_time
  FROM
    filtered);

CREATE OR REPLACE VIEW degrees AS (
  SELECT
    count(node) AS degree,
    max(created_time) AS created_time
  FROM
    nodes
  GROUP BY
    node
  ORDER BY
    degree);

CREATE OR REPLACE FUNCTION insert_rolling_medians ()
  RETURNS TRIGGER
  AS $$
BEGIN
  INSERT INTO rolling_medians (
    SELECT
      nextval('median_order') AS id,
      median (degree) AS calc_median,
      max(created_time) AS created_time
    FROM
      degrees);
  RETURN NULL;
END;
$$
LANGUAGE PLPGSQL;

CREATE TRIGGER do_insert_rolling_medians
  AFTER INSERT ON raw_transactions
  FOR EACH ROW
  EXECUTE FUNCTION insert_rolling_medians ();

