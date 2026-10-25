import os

# Doris usa mysql+mysqldb://; pymysql actúa como shim si mysqlclient no está disponible
try:
    import MySQLdb  # noqa: F401
except ImportError:
    try:
        import pymysql
        pymysql.install_as_MySQLdb()
    except ImportError:
        pass

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "olist-superset-secret-change-in-prod")
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_META_DB_URI",
    "sqlite:////app/superset_home/superset.db",
)

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS_SET": True,
    "EMBEDDED_SUPERSET": True,
}

LANGUAGES = {
    "es": {"flag": "es", "name": "Español"},
    "en": {"flag": "us", "name": "English"},
}
BABEL_DEFAULT_LOCALE = "es"

TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = False

MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY", "")

# ── Paleta OLIST ──────────────────────────────────────────────────────────────
EXTRA_CATEGORICAL_COLOR_SCHEMES = [
    {
        "id": "olist",
        "description": "Paleta OLIST Logística",
        "label": "OLIST",
        "isDefault": False,
        "colors": [
            "#1F5FC4",  # azul — a tiempo, favorable
            "#B8571B",  # naranja — tardío, alerta
            "#5A6270",  # gris — referencia
            "#9DB8E6",  # azul claro — secundario
            "#E3A27A",  # naranja claro — advertencia
            "#0F766E",  # verde — OK
            "#4B5563",  # gris oscuro
            "#D97706",  # ámbar
        ],
    }
]

# ── Sanitización HTML: permitir SVG en plantillas Handlebars ─────────────────
# Superset 6.1 usa DOMPurify en el cliente con esta config (tags → ADD_TAGS,
# attributes values aplanados → ADD_ATTR).  No desactiva la sanitización.
HTML_SANITIZATION_SCHEMA_EXTENSIONS = {
    "tags": ["svg", "polyline"],
    "attributes": {
        "svg": ["viewBox", "preserveAspectRatio"],
        "polyline": ["points"],
    },
}

EXTRA_SEQUENTIAL_COLOR_SCHEMES = [
    {
        "id": "olist_otd",
        "description": "Escala divergente OTD (rojo → azul)",
        "label": "OLIST OTD",
        "isDiverging": True,
        "isDefault": False,
        "colors": [
            "#C0221E",  # OTD baja — alerta (rojo)
            "#E07A7A",  # rojo claro
            "#F5F0E8",  # neutro
            "#9DB8E6",  # azul claro
            "#1F5FC4",  # OTD alta — favorable
        ],
    },
    {
        "id": "olist_tardios",
        "description": "Escala secuencial tardíos (beige → marrón)",
        "label": "OLIST Tardíos",
        "isDiverging": False,
        "isDefault": False,
        "colors": [
            "#FBEAE0",  # pocos tardíos
            "#F0C4A8",
            "#D9906A",
            "#B3602E",
            "#8A3A10",  # muchos tardíos
        ],
    },
]
