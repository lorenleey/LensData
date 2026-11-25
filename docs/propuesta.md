# LENSDATA · Análisis del rendimiento de vídeos de una tienda

**Periodo previsto:** 10 de noviembre – 2 de diciembre de 2026

## Propuesta

¿Qué categorías de producto generan más interacción en TikTok e Instagram cuando sus vídeos llevan el mismo tiempo publicados? lensData dará al equipo de contenidos de una tienda de variedades un panel para explorar esta pregunta y orientar la planificación de sus próximas publicaciones.

La solución se desarrollará en Databricks Free Edition con una muestra sintética de 40 publicaciones y 12 días de seguimiento. La muestra cubrirá regalos, comida asiática, belleza y juguetes, con cinco publicaciones por categoría en cada plataforma. El panel permitirá consultar la evolución de cada vídeo, comparar categorías dentro de una red y revisar los totales por plataforma.

## Objetivos

- **Comparar el rendimiento en condiciones equivalentes.** Medir la interacción por cada 100 reproducciones al tercer día desde la publicación. Cada comparación indicará cuántos vídeos tienen una medición a esa edad.
- **Conservar el historial de cada publicación.** Identificar cada medición por plataforma, vídeo y fecha. Una corrección actualizará el día correspondiente y el reenvío de una versión anterior conservará el valor corregido.
- **Hacer trazable la calidad de los datos.** Conservar los valores de origen, normalizar las categorías y registrar el motivo de cada rechazo, incluida cualquier medición que haga referencia a un vídeo desconocido.
- **Automatizar la actualización del panel.** Encadenar la validación de archivos, la carga y la publicación de resultados en un Lakeflow Job. El historial permitirá identificar la tarea fallida y reparar la ejecución una vez corregida la entrada.

## Fases y resultados previstos

| Fechas | Fase | Resultado previsto |
|---|---|---|
| 10–12 nov | Preparación del entorno | Configuración del espacio de trabajo, lectura de un CSV desde un notebook y creación de la primera tabla Delta. |
| 13–16 nov | Generación e ingesta | Muestra reproducible y carga incremental con Auto Loader. La capa Bronze conservará las filas de origen y el archivo del que procede cada una. |
| 17–20 nov | Limpieza y transformación | Publicaciones y mediciones tipadas en Silver, con categorías normalizadas y registros rechazados identificables. |
| 21–24 nov | Consolidación incremental | Actualización mediante MERGE y tres tablas Gold por vídeo, categoría y plataforma. Se comprobarán la segunda carga, su repetición y una corrección tardía. |
| 25–27 nov | Orquestación y visualización | Panel con tres visualizaciones y ejecución completa del job. Una prueba con una cabecera incorrecta documentará el fallo y la recuperación. |
| 28–29 nov | Validación final | Cierre de funcionalidades, repetición de las pruebas y documentación del procedimiento con diagramas y evidencias. |
| 30 nov–1 dic | Preparación de la entrega | Informe técnico y presentación con resultados verificados. Revisión final del repositorio y de los enlaces. |
| 2 dic | Entrega | Repositorio, documentación, presentación y evidencias de ejecución disponibles para su revisión. |
