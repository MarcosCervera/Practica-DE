-- Crea tablas para prod --

CREATE TABLE bcra(
	fecha Date PRIMARY KEY,
	base INT,
	cajas_ahorro INT,
	cer FLOAT,
	circulacion_monetaria INT,
	cuentas_corrientes INT,
	depositos INT,
	inflacion_interanual_oficial FLOAT,
	inflacion_mensual_oficial FLOAT,
	plazo_fijo INT,
	reservas INT,
	usd varchar(50),
	usd_of varchar(50),
	usd_of_minorista varchar(50),
	uva FLOAT
);


-- Procedimiento para cargar tabla--
	
CREATE OR REPLACE PROCEDURE MoveDataToProd () 
LANGUAGE plpgsql
AS $$
BEGIN
	DELETE FROM bcra ;
	INSERT INTO bcra (
			fecha,
			base,
			cajas_ahorro,
			cer,
			circulacion_monetaria,
			cuentas_corrientes,
			depositos,
			inflacion_interanual_oficial,
			inflacion_mensual_oficial,
			plazo_fijo,
			reservas,
			usd,usd_of,
			usd_of_minorista,
			uva
		)
	SELECT 
		fecha, 
		CAST(REGEXP_REPLACE(base, '\.0$', '') AS INT) as base, 
		CAST(REGEXP_REPLACE(cajas_ahorro, '\.0$', '') AS INT) as cajas_ahorro, 
		CAST(cer AS FLOAT) as cer,
		CAST(REGEXP_REPLACE(circulacion_monetaria, '\.0$', '') AS INT) as circulacion_monetaria, 
		CAST(REGEXP_REPLACE(cuentas_corrientes, '\.0$', '') AS INT) as cuentas_corrientes, 
		CAST(REGEXP_REPLACE(depositos, '\.0$', '') AS INT) as depositos, 
		CAST(inflacion_interanual_oficial AS FLOAT) as inflacion_interanual_oficial,
		CAST(inflacion_mensual_oficial AS FLOAT) as inflacion_mensual_oficial,
		CAST(REGEXP_REPLACE(plazo_fijo, '\.0$', '') AS INT) as plazo_fijo, 
		CAST(REGEXP_REPLACE(reservas, '\.0$', '') AS INT) as reservas, 
		usd_hash as usd ,
		usd_of_hash  as usd_of ,
		usd_of_minorista_hash as usd_of_minorista,
		CAST(uva AS FLOAT) as uva
	FROM stage_bcra_hash;
END;
$$;

CALL MoveDataToProd();


select *
from bcra