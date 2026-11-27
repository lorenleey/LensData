from pathlib import Path
import json

# Ejecutar desde la raíz de LensData. Esto conserva las celdas y las salidas existentes.
paths = [
    'notebooks/bronze/01_ingestar_publicaciones.ipynb',
    'notebooks/bronze/02_ingestar_mediciones.ipynb',
    'notebooks/silver/01_transformar_silver.ipynb',
    'notebooks/silver/02_merge_mediciones.ipynb',
    'notebooks/gold/01_generar_gold.ipynb',
    'tests/test_gold.ipynb',
]
widget = "try:\n    dbutils.widgets.text('project_root', '')\n    job_root = dbutils.widgets.get('project_root')\nexcept (NameError, AttributeError):\n    job_root = ''\n"
for name in paths:
    path = Path(name)
    nb = json.loads(path.read_text(encoding='utf-8'))
    found = False
    for cell in nb['cells']:
        if cell['cell_type'] != 'code':
            continue
        s = ''.join(cell['source'])
        if 'Path.cwd()' not in s or 'from pathlib import Path' not in s:
            continue
        found = True
        if 'job_root =' not in s:
            s = s.replace('from pathlib import Path\n', 'from pathlib import Path\n\n' + widget, 1)
            s = s.replace('(Path.cwd(), *Path.cwd().parents)', '(Path(job_root),) if job_root else (Path.cwd(), *Path.cwd().parents)')
            # Parenthesize conditional iterable inside comprehension.
            s = s.replace('in (Path(job_root),) if job_root else (Path.cwd(), *Path.cwd().parents)', 'in ((Path(job_root),) if job_root else (Path.cwd(), *Path.cwd().parents))')
            cell['source'] = s.splitlines(True)
            import ast
            ast.parse(s)
        break
    if not found:
        raise RuntimeError(f'No se encontró la configuración en {name}')
    path.write_text(json.dumps(nb, ensure_ascii=False, indent=1) + '\n', encoding='utf-8')
    print('Ruta de proyecto preparada:', name)
