"""
voronoi_cep.py
Genera polígonos Voronoi por prefijo CEP para visualizar en Kepler.gl.
Lee geo_cep_silver de RustFS, calcula Voronoi con scipy (sin shapely),
recorta al bounding box de Brasil y exporta GeoJSON + CSV con WKT.
"""
import os, json
import numpy as np
import pandas as pd
from scipy.spatial import Voronoi
from pyspark.sql import SparkSession

# ── Spark ───────────────────────────────────────────────────────────────────
RUSTFS  = os.environ.get('RUSTFS_ENDPOINT',    'http://rustfs:9000')
S3_KEY  = os.environ.get('AWS_ACCESS_KEY_ID',  'rustfsadmin')
S3_SEC  = os.environ.get('AWS_SECRET_ACCESS_KEY', 'rustfsadmin123')
JARS    = ','.join([
    '/opt/spark/extra-jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar',
    '/opt/spark/extra-jars/hadoop-aws-3.4.2.jar',
    '/opt/spark/extra-jars/aws-sdk-bundle-v2-2.29.52.jar',
])

spark = (
    SparkSession.builder.appName('voronoi-cep').master('local[4]')
    .config('spark.jars', JARS)
    .config('spark.hadoop.fs.s3a.impl',                   'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.hadoop.fs.s3a.endpoint',               RUSTFS)
    .config('spark.hadoop.fs.s3a.access.key',             S3_KEY)
    .config('spark.hadoop.fs.s3a.secret.key',             S3_SEC)
    .config('spark.hadoop.fs.s3a.path.style.access',      'true')
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('ERROR')

df = spark.read.parquet('s3a://olist/silver/geo_cep').toPandas()
spark.stop()
print(f"geo_cep_silver: {len(df):,} prefijos  columnas: {list(df.columns)}")

# ── Filtrar al bbox de Brasil ────────────────────────────────────────────────
LNG_MIN, LNG_MAX = -74.0, -28.0
LAT_MIN, LAT_MAX =  -34.0,  5.3

df = df[
    (df.lat_med >= LAT_MIN) & (df.lat_med <= LAT_MAX) &
    (df.lng_med >= LNG_MIN) & (df.lng_med <= LNG_MAX)
].copy().reset_index(drop=True)
print(f"Dentro de Brasil: {len(df):,} prefijos")

# ── Puntos (x=lng, y=lat) + puntos "fantasma" para cerrar regiones abiertas ─
pts = df[['lng_med', 'lat_med']].values
n   = len(pts)

cx = (LNG_MIN + LNG_MAX) / 2
cy = (LAT_MIN + LAT_MAX) / 2
M  = 15.0   # margen generoso

ghosts = []
for lng in np.linspace(LNG_MIN - M, LNG_MAX + M, 12):
    for lat in [LAT_MIN - M, LAT_MAX + M]:
        ghosts.append([lng, lat])
for lat in np.linspace(LAT_MIN - M, LAT_MAX + M, 12):
    for lng in [LNG_MIN - M, LNG_MAX + M]:
        ghosts.append([lng, lat])

all_pts = np.vstack([pts, np.array(ghosts)])
vor     = Voronoi(all_pts)
print(f"Voronoi calculado: {len(vor.vertices):,} vértices")

# ── Sutherland-Hodgman: recorte de polígono convexo a bbox ──────────────────
def clip_poly(poly):
    """Recorta lista de puntos (x,y) al bbox. Voronoi cells son convexas."""
    def inside(p, edge):
        x, y = p
        if   edge == 'L': return x >= LNG_MIN
        elif edge == 'R': return x <= LNG_MAX
        elif edge == 'B': return y >= LAT_MIN
        elif edge == 'T': return y <= LAT_MAX

    def intersect(a, b, edge):
        ax, ay = a; bx, by = b
        dx, dy = bx - ax, by - ay
        if   edge == 'L': t = (LNG_MIN - ax) / dx if dx else 0; return (LNG_MIN, ay + t*dy)
        elif edge == 'R': t = (LNG_MAX - ax) / dx if dx else 0; return (LNG_MAX, ay + t*dy)
        elif edge == 'B': t = (LAT_MIN - ay) / dy if dy else 0; return (ax + t*dx, LAT_MIN)
        elif edge == 'T': t = (LAT_MAX - ay) / dy if dy else 0; return (ax + t*dx, LAT_MAX)

    out = list(poly)
    for edge in ('L', 'R', 'B', 'T'):
        if not out:
            break
        inp, out = out, []
        for i, cur in enumerate(inp):
            prev = inp[i - 1]
            if inside(cur, edge):
                if not inside(prev, edge):
                    out.append(intersect(prev, cur, edge))
                out.append(cur)
            elif inside(prev, edge):
                out.append(intersect(prev, cur, edge))
    return out

# ── Construir GeoJSON ────────────────────────────────────────────────────────
zip_col = 'zip' if 'zip' in df.columns else df.columns[0]

features   = []
n_skip_inf = 0
n_skip_deg = 0

for i in range(n):
    ridx  = vor.point_region[i]
    vidxs = vor.regions[ridx]

    if -1 in vidxs or len(vidxs) < 3:
        n_skip_inf += 1
        continue

    poly    = vor.vertices[vidxs].tolist()   # lista de (lng, lat)
    clipped = clip_poly(poly)

    if len(clipped) < 3:
        n_skip_deg += 1
        continue

    # Redondear a 4 decimales (~11 m) para reducir tamaño
    ring = [[round(p[0], 4), round(p[1], 4)] for p in clipped]
    ring.append(ring[0])   # cerrar anillo

    features.append({
        'type': 'Feature',
        'geometry': {'type': 'Polygon', 'coordinates': [ring]},
        'properties': {
            'zip_prefix': str(df.at[i, zip_col]),
            'lat_med':    round(float(df.at[i, 'lat_med']), 5),
            'lng_med':    round(float(df.at[i, 'lng_med']), 5),
        }
    })

print(f"Polígonos generados : {len(features):,}")
print(f"Saltados (infinitos): {n_skip_inf:,}  |  degenerados: {n_skip_deg:,}")

# ── Exportar GeoJSON ─────────────────────────────────────────────────────────
OUT_DIR  = '/home/notebooks/graficos'
GEOJSON  = f'{OUT_DIR}/voronoi_cep.geojson'
geojson  = {'type': 'FeatureCollection', 'features': features}

with open(GEOJSON, 'w') as f:
    json.dump(geojson, f, separators=(',', ':'))

size_mb = os.path.getsize(GEOJSON) / 1e6
print(f"\nGeoJSON → {GEOJSON}  ({size_mb:.1f} MB)")

# ── Exportar CSV (zip_prefix, lat_med, lng_med, wkt) ────────────────────────
# WKT simple: POLYGON ((lng lat, ...))
def to_wkt(ring):
    coords = ' '.join(f'{p[0]} {p[1]}' for p in ring)
    return f'POLYGON (({coords}))'

rows = [
    {
        'zip_prefix': f['properties']['zip_prefix'],
        'lat_med':    f['properties']['lat_med'],
        'lng_med':    f['properties']['lng_med'],
        'geometry':   to_wkt(f['geometry']['coordinates'][0]),
    }
    for f in features
]
CSV = f'{OUT_DIR}/voronoi_cep.csv'
pd.DataFrame(rows).to_csv(CSV, index=False)
size_csv = os.path.getsize(CSV) / 1e6
print(f"CSV     → {CSV}  ({size_csv:.1f} MB)")
print("\nListo. Cargar voronoi_cep.geojson directamente en Kepler.gl (Add Data → GeoJSON).")
