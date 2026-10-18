WITH seleccionado AS (
 SELECT *
 FROM mercedes.gold.procesos_dashboard
 WHERE stud_id = :stud_id
 ORDER BY fecha_hora_proceso DESC, uniqueid
 LIMIT 1
)
SELECT orden, campo, valor
FROM seleccionado
LATERAL VIEW STACK(
 11,
 1, 'Q-Status', CAST(quality AS STRING),
 2, 'Date', DATE_FORMAT(fecha_proceso, 'dd.MM.yyyy'),
 3, 'Time', CAST(hora_proceso AS STRING),
 4, 'Joining Spot Series',
 CAST(joining_spot_series AS STRING),
 5, 'Stud ID', CAST(stud_id AS STRING),
 6, 'Stud type', CAST(stud_type AS STRING),
 7, 'Carbody ID', CAST(carbody_id AS STRING),
 8, 'Type', CAST(tipo_proceso AS STRING),
 9, 'Version', CAST(version_proceso AS STRING),
 10, 'Error code', CAST(fault_code AS STRING),
 11, 'Remark Tucker_TE', CAST(remarks AS STRING)
) detalle AS orden, campo, valor
ORDER BY orden;
