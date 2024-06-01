

--- es la tecnica mas sencilla (aclarado y rellenado) pero no es la unica tecnica (e.g actualizacion)
CREATE PROCEDURE pETL_Desastres
AS

DELETE FROM z_marcos_f_DESASTRES_FINAL;

INSERT INTO z_marcos_f_DESASTRES_FINAL
SELECT 
	x.Cuatrenio, 
	AVG(x.Temperatura) AS Temp_AVG, 
	AVG(x.NivelOxigeno) AS Oxi_AVG,
	SUM(x.Tsunamis) AS T_Tsunamis,
	SUM(x.OlasCalor) AS T_OlasCalor,
	SUM(x.Terremotos) AS T_Terremotos,
	SUM(x.Erupciones) AS T_Erupciones,
	SUM(x.Incendios) AS T_Incendios,
	AVG(x.Muertes_Jovenes) as M_Jovenes_AVG,
	AVG(x.Muertes_Adultos) as M_Adultos_AVG, 
	AVG(x.Muertes_Ancianos) as M_Ancianos_AVG
FROM
(SELECT 
	CASE WHEN c.año < 2026 THEN '2023-2026' ELSE '2027-2030' END AS Cuatrenio,
	Temperatura =c.temperatura,
	NivelOxigeno =c.oxigeno,
	Tsunamis= d.Tsunamis,
	OlasCalor= d.Olas_calor,
	Terremotos= d.Terremotos,
	Erupciones= d.Erupciones,
	Incendios=d.Incendios, 
	Muertes_Jovenes= m.R_Menor15 + m.R_15_a_30,
	Muertes_Adultos= m.R_30_a_45 +m.R_45_a_60,
	Muertes_Ancianos= m.R_M_a_60
	FROM z_marcos_clima as c
		JOIN z_marcos_desastres as d ON c.año =d.año
		JOIN z_marcos_muertes as m ON c.año = m.año) x
	GROUP BY x.Cuatrenio
GO

-- ir a Programatically>> Stores Procedures y verificar que se creo el procedimeinto

--7. Executar procedimeinto
EXECUTE pETL_Desastres;
GO

-- 8. Verificar que se tiene el resultado


SELECT * FROM z_marcos_f_DESASTRES_FINAL
GO

