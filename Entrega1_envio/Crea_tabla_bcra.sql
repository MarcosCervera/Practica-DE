SHOW TABLES
FROM
SCHEMA "data-engineer-database"."marcoscervera_coderhouse";

DROP TABLE IF EXISTS marcoscervera_coderhouse.stage_bcra;

CREATE TABLE stage_bcra(
	fecha	VARCHAR(200),
    usd   VARCHAR(50),
    usd_of         VARCHAR(50)
);


SELECT  
*
FROM marcoscervera_coderhouse.stage_bcra
where usd_of is not null
order by usd_of desc