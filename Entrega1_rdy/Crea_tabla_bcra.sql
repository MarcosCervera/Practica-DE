SHOW TABLES
FROM
SCHEMA "data-engineer-database"."marcoscervera_coderhouse";

DROP TABLE IF EXISTS marcoscervera_coderhouse.stage_bcra;

CREATE TABLE stage_bcra(
	fecha	VARCHAR(200),
    usd   VARCHAR(50),
    usd_of         VARCHAR(50)
);


DROP TABLE IF EXISTS marcoscervera_coderhouse.stage_bcra_full;

CREATE TABLE stage_bcra_full(
	fecha Date,
	base VARCHAR(50),
	cajas_ahorro VARCHAR(50),
	cer VARCHAR(50),
	circulacion_monetaria VARCHAR(50),
	cuentas_corrientes VARCHAR(50),
	depositos VARCHAR(50),
	inflacion_interanual_oficial VARCHAR(50),
	inflacion_mensual_oficial VARCHAR(50),
	plazo_fijo VARCHAR(50),
	reservas VARCHAR(50),
	usd VARCHAR(50),
	usd_of VARCHAR(50),
	usd_of_minorista VARCHAR(50),
	uva VARCHAR(50)
);


select *
from stage_bcra_full



SELECT  
*
FROM marcoscervera_coderhouse.stage_bcra
where usd_of is not null
order by usd_of desc