WITH seleccionado AS (
    SELECT *
    FROM mercedes.gold.procesos_dashboard
    WHERE stud_id = :stud_id
    ORDER BY fecha_hora_proceso DESC, uniqueid
    LIMIT 1
),

-- Valores fijos tomados de la captura para el ejercicio.
referencias AS (
    SELECT
        parametro,
        CAST(referencia AS DOUBLE) AS reference_value,
        CAST(minimo AS DOUBLE) AS min_value,
        CAST(maximo AS DOUBLE) AS max_value
    FROM VALUES
        ('Weld voltage',  NULL,  15.0,  35.0),
        ('Weld current', 600.0,  NULL,  NULL),
        ('Weld time',     60.0,  56.0,  64.0),
        ('Weld energy',   NULL,  NULL, 1600.0),
        ('Lift Position', 2.5,   2.4,   2.6),
        ('Penetration',   -0.7,  -0.8,  -0.5)
    AS configuracion(parametro, referencia, minimo, maximo)
),

medidas_proceso AS (
    SELECT
        orden,
        parametro,
        unidad,
        actual_value
    FROM seleccionado
    LATERAL VIEW STACK(
        15,
        1, 'Pilot voltage', 'V',
           pilot_voltage_v,
        2, 'Weld voltage', 'V',
           weld_voltage_v,
        3, 'Weld current', 'A',
           weld_current_a,
        4, 'Weld time', 'ms',
           CAST(weld_time_ms AS DOUBLE),
        5, 'Weld energy', 'J',
           CAST(weld_energy_j AS DOUBLE),
        6, 'Lift Position', 'mm',
           lift_height_mm,
        7, 'Penetration', 'mm',
           penetration_mm,
        8, 'Weld tool slide forward move time', 'ms',
           CAST(slide_forward_move_time_ms AS DOUBLE),
        9, 'Weld tool slide backward move time', 'ms',
           CAST(slide_backward_move_time_ms AS DOUBLE),
        10, 'Feeding time', 'ms',
            CAST(feeding_time_ms AS DOUBLE),
        11, 'Feeding time stud in tube', 'ms',
            CAST(feeding_time_stud_in_tube_ms AS DOUBLE),
        12, 'Weld tool move time loading pin backward', 'ms',
            CAST(loading_pin_backward_time_ms AS DOUBLE),
        13, 'Weld tool move time loading pin forward', 'ms',
            CAST(loading_pin_forward_time_ms AS DOUBLE),
        14, 'Actual Position', 'mm',
            actual_position_mm,
        15, 'Reference Position', 'mm',
            reference_position_mm
    ) medidas AS orden, parametro, unidad, actual_value
)

SELECT
    m.orden,
    m.parametro,
    m.unidad,
    r.reference_value,
    r.min_value,
    r.max_value,
    m.actual_value,
    CASE
        WHEN m.actual_value IS NULL
            THEN 'Sin dato'
        WHEN r.min_value IS NULL AND r.max_value IS NULL
            THEN 'Sin límites'
        WHEN (r.min_value IS NOT NULL
              AND m.actual_value < r.min_value)
          OR (r.max_value IS NOT NULL
              AND m.actual_value > r.max_value)
            THEN 'Fuera de rango'
        ELSE 'Dentro de rango'
    END AS estado
FROM medidas_proceso m
LEFT JOIN referencias r
    ON m.parametro = r.parametro
ORDER BY m.orden;
