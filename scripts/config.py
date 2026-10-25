# Configuración compartida — TP Integrador Olist
# Fecha de corte simulada para scoring en tiempo real
CUTOFF_DATE = "2018-06-15"

# Partición temporal del modelo
FECHA_FIN_TRAIN = "2017-12-31"
FECHA_FIN_VAL   = "2018-03-31"
FECHA_FIN_TEST  = "2018-08-29"

# Modelo
MODEL_NAME    = "olist_tarde_logistica"
MODEL_VERSION = 1

# Umbrales de percentil para nivel de riesgo (vs distribución de validación)
PERCENTIL_ALTO  = 90
PERCENTIL_MEDIO = 70
