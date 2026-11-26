# Documentación de LensData

## Referencias de estilo

Antes de modificar `README.md`, revisar los README de los proyectos Libris y moduloBigData de Lorenley como referencia. Mantener la identidad de LensData y adaptar la estructura al estado real del proyecto.

## README.md

- Escribir en español, con términos técnicos en inglés cuando corresponda. Usar una voz natural, técnica, directa y profesional, como una desarrolladora que presenta su proyecto.
- Mantener un diseño limpio con Markdown nativo de GitHub, títulos claros y párrafos breves. Usar emojis con moderación, badges solo para las tecnologías principales, tablas compactas y Mermaid cuando aclaren la arquitectura o el flujo.
- Presentar con rapidez el problema de negocio, el objetivo, el funcionamiento, el stack, las funcionalidades terminadas y los resultados comprobados. Añadir estructura del repositorio, ejecución y documentación cuando aporten información concreta. No crear secciones vacías ni contenido de relleno.
- Incluir únicamente funcionalidades implementadas y resultados validados. No enumerar tareas pendientes ni mejoras futuras en README.md o desarrollo.md; conservar la planificación en la propuesta. No inventar métricas, pruebas o capturas.
- Enlazar solo capturas reales y representativas, con rutas relativas y texto alternativo descriptivo. Conservar las capturas existentes en `img/`. Si se organizan nuevas capturas, usar bloques amplios como `img/entorno/`, `img/bronze/`, `img/silver/` y `img/gold/`, sin carpetas por fase. Dejar el conjunto detallado de imágenes en `desarrollo.md`.
- Tras cada tarea, actualizar solo las secciones afectadas. Conservar la estructura y evitar que el README crezca con detalles repetidos del proceso.

## desarrollo.md

- Documentar fases y tareas, decisiones y motivos, conceptos nuevos, fragmentos de código relevantes, incidencias y soluciones, validaciones reales, capturas y limitaciones.
- Documentar resultados observados y distinguir pruebas simuladas de datos reales. Situar las capturas del proceso junto al paso que prueban.
- No duplicar el README: este archivo es el registro técnico y académico; el README es la presentación breve del proyecto.

## Control de versiones

- Cuando corresponda y exista autorización para confirmar cambios, separar los commits de documentación de los de código. Usar mensajes breves, descriptivos, en español y en minúsculas.
- No hacer commit ni push si se ha pedido revisar o validar los cambios antes.
