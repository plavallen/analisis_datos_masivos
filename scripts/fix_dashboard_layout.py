"""
fix_dashboard_layout.py
=======================
Repara el layout del dashboard Superset cuando el import del ZIP produce
nodos CHART duplicados (48 en lugar de 24).

PROBLEMA RAÍZ
-------------
El YAML exportado al repo (superset/olist_dashboard/dashboards/OLIST_Logistica.yaml)
contiene 48 nodos CHART en su campo `position`:
  - 24 nodos SIN sliceName (CHART-9260FCE8, etc.): correctamente posicionados
    en el árbol de tabs/rows/columns, con los w/h correctos.
  - 24 nodos CON sliceName (CHART-J1W3IKWJ, etc.): todos metidos en un único
    ROW plano (ROW-N-OJUC2PZ5) fuera de los tabs, con w/h incorrectos para
    las posiciones de los tabs.

Cuando se ejecuta `superset import-dashboards -p olist_dashboard.zip`,
Superset importa ese YAML con los 48 nodos tal cual, produciendo:
  - Charts sin título visible (los sin-sliceName)
  - Una fila extra al pie de cada tab con los charts del flat-row
  - Charts con tamaños incorrectos

SOLUCIÓN
--------
1. Tomar el `position` del YAML como base.
2. CONSERVAR los nodos sin-slug (tienen las dimensiones correctas).
3. Copiar el sliceName del nodo-slug correspondiente al nodo sin-slug.
4. ELIMINAR los nodos con-slug (estaban en el flat-row).
5. ELIMINAR ROW-N-OJUC2PZ5 y limpiar refs en children.
6. PUT el position_json resultante al dashboard via API.

CUÁNDO CORRERLO
---------------
Después de cualquier `superset import-dashboards` que haya dejado el
dashboard con charts duplicados o con una fila extra al pie.

USO
---
    python3 fix_dashboard_layout.py <SUPERSET_TOKEN>

Para obtener el token:
    TOKEN=$(curl -s -X POST http://localhost:8088/api/v1/security/login \
      -H 'Content-Type: application/json' \
      -d '{"username":"admin","password":"admin","provider":"db","refresh":true}' \
      | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
    python3 fix_dashboard_layout.py "$TOKEN"

SOLUCIÓN DEFINITIVA (para que el import sea limpio de raíz)
-----------------------------------------------------------
Después de correr este script, re-exportar el dashboard limpio y guardar el ZIP:

    docker compose -f etapa2/docker-compose.yml exec superset \
      superset export-dashboards -f /tmp/olist_dashboard_clean.zip
    docker compose -f etapa2/docker-compose.yml cp \
      superset:/tmp/olist_dashboard_clean.zip superset/olist_dashboard.zip

Luego re-extraer el YAML y commitear, para que el próximo import sea limpio.
"""

import sys, json, yaml, requests
from collections import defaultdict

YAML_PATH = 'superset/olist_dashboard/dashboards/OLIST_Logistica.yaml'
BASE      = 'http://localhost:8088'
EXTRA_ROW = 'ROW-N-OJUC2PZ5'

def main():
    if len(sys.argv) < 2:
        print("Uso: python3 fix_dashboard_layout.py <TOKEN>")
        sys.exit(1)

    TOKEN   = sys.argv[1]
    HEADERS = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}

    # 1. Cargar position del YAML
    with open(YAML_PATH) as f:
        d = yaml.safe_load(f)
    pos = json.loads(json.dumps(d.get('position', {})))

    # 2. Identificar pares (chartId → {no_slug_id, slug_id, slug_name})
    by_chart = defaultdict(dict)
    for nid, node in pos.items():
        if isinstance(node, dict) and node.get('type') == 'CHART':
            cid  = node.get('meta', {}).get('chartId')
            slug = node.get('meta', {}).get('sliceName', '')
            if slug:
                by_chart[cid]['slug_id']   = nid
                by_chart[cid]['slug_name'] = slug
            else:
                by_chart[cid]['no_slug_id'] = nid

    # 3. Copiar sliceName al nodo sin-slug
    for cid, m in by_chart.items():
        if 'no_slug_id' in m and 'slug_name' in m:
            pos[m['no_slug_id']]['meta']['sliceName'] = m['slug_name']

    # 4. Eliminar nodos con-slug
    slug_ids = {m['slug_id'] for m in by_chart.values() if 'slug_id' in m}
    for sid in slug_ids:
        pos.pop(sid, None)

    # 5. Limpiar referencias en children
    for node in pos.values():
        if isinstance(node, dict) and 'children' in node:
            node['children'] = [c for c in node['children'] if c not in slug_ids]

    # 6. Eliminar EXTRA_ROW
    pos.pop(EXTRA_ROW, None)
    grid = pos.get('GRID_ID', {})
    if isinstance(grid, dict):
        grid['children'] = [c for c in grid.get('children', []) if c != EXTRA_ROW]

    # 7. Limpiar ROWs vacíos
    empty = {k for k,v in pos.items()
             if isinstance(v, dict) and v.get('type') == 'ROW' and not v.get('children')}
    for nid in empty:
        pos.pop(nid)
    for node in pos.values():
        if isinstance(node, dict) and 'children' in node:
            node['children'] = [c for c in node['children'] if c not in empty]

    # 8. Verificar
    by_chart2 = defaultdict(list)
    for k, v in pos.items():
        if isinstance(v, dict) and v.get('type') == 'CHART':
            by_chart2[v.get('meta', {}).get('chartId')].append(k)

    def count_charts(nid, seen=None):
        if seen is None: seen = set()
        if nid in seen: return 0
        seen.add(nid)
        node = pos.get(nid, {})
        if not isinstance(node, dict): return 0
        if node.get('type') == 'CHART': return 1
        return sum(count_charts(c, seen) for c in node.get('children', []))

    print("Charts por tab:")
    for tid, tab in sorted((k, v) for k,v in pos.items()
                            if isinstance(v, dict) and v.get('type') == 'TAB'):
        txt = tab.get('meta', {}).get('text', '')
        print(f"  \"{txt}\": {count_charts(tid)} charts")

    dups = {k: v for k, v in by_chart2.items() if len(v) > 1}
    print(f"Duplicados: {len(dups)}")
    if dups:
        print("  ATENCIÓN:", dups)

    # 9. PUT al dashboard
    r = requests.get(f'{BASE}/api/v1/dashboard/olist', headers=HEADERS)
    dashboard_id = r.json()['result']['id']

    r2 = requests.put(f'{BASE}/api/v1/dashboard/{dashboard_id}', headers=HEADERS,
                      json={'position_json': json.dumps(pos)})
    print(f"PUT → {r2.status_code}", "OK" if r2.status_code in (200,201) else r2.text[:200])


if __name__ == '__main__':
    main()
