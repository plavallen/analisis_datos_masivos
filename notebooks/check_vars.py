"""Verifica qué variables necesarias están en el kernel."""
import jupyter_client, time

CF = '/root/.local/share/jupyter/runtime/kernel-7c95adf6-3f2e-4431-bc02-21060b7bac4a.json'

kc = jupyter_client.BlockingKernelClient()
kc.load_connection_file(CF)
kc.start_channels()
kc.wait_for_ready(timeout=15)
print('Kernel conectado.')

CHECK_CODE = """
vars_check = [
    'tramos_agg', 'envios_tramo', 'envios_ok', 'n_ok',
    'box_stats', 'pedidos_tardios', 'n_base', 'tasa_mes',
    'dosis', 'n_pr', 'k09', 'pct_neg_global',
    'cmp', 'score_dist', 'status_dist', 'n_todos',
    'tasa_canc_mes', 'corr_array', 'FEATURE_COLS', 'n_corr',
    'vendor_class', 'p0_global', 'Z95', 'Z998',
]
missing = [v for v in vars_check if v not in dir()]
present = [v for v in vars_check if v in dir()]
print('PRESENTES:', present)
print('AUSENTES:', missing)
"""

msg_id = kc.execute(CHECK_CODE)
while True:
    msg = kc.get_iopub_msg(timeout=30)
    ph = msg.get('parent_header', {})
    if ph.get('msg_id') != msg_id:
        continue
    if msg['msg_type'] == 'stream':
        print(msg['content']['text'])
    elif msg['msg_type'] == 'status' and msg['content']['execution_state'] == 'idle':
        break

kc.stop_channels()
