# Proyecto de Data Engineering: Agregación de ofertas de trabajos de tecnología de la página getonbrd.cl
Este proyecto consiste en crear una pipeline ETL, extrayendo la información desde la página getonbrd.cl para luego presentarla en un tablero de reporte que se actualiza constantemente

Las tecnologías usadas fueron:
- API de getonbrd.cl
- Python
- BigQuery
- Looker Studio
- Airflow
- Bash Scripts

## Funcionamiento de la pipeline:

### Diagrama Explicativo
![Diagrama Explicativo](https://github.com/sebakjal/first_DE_project/blob/main/DiagramaProyecto1.png)

El primer paso es extraer las ofertas de trabajo publicadas durante el día desde la API de getonbrd.cl usando los módulos *requests* y *json* en un script de Python (. Este mismo script integra la información de interés a un Dataframe de Pandas, y luego se exporta a un archivo .csv con nombre único. El archivo .csv correspondiente al presente día se importa a una tabla en BigQuery (previamente creada) a través de la CLI de Google Cloud, actuando esta como base de datos. 

La pipeline está automatizada con Airflow, y corre los scripts de Python y Bash diariamente para poblar la base de datos, enviando un correo cuando el proceso falla en algún punto.

## Dashboard:
A partir de la base de datos se crea un tablero en Looker Studio (anteriormente Google Data Studio). Este muestra varios indicadores sacados desde el análisis de las ofertas de trabajo posteadas en el sitio, como popularidad de los lenguajes de programación mencionados en los requisitos, salarios según seniority, preferencias de plataformas de la nube, entre otros.

### Ejemplo del Tablero
![Ejemplo Tablero](https://github.com/sebakjal/first_DE_project/blob/main/TableroEjemplo.png)
