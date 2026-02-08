En este proyecto se presenta lo realizado en el ejercicio del Módulo 2 de una capacitacion de Analisis de Datos, cubriendo desde el 
modelado de datos hasta la visualización en Power BI.  <br>
Fuentes de Datos: Generados a partir de un Script de Python tomando como guía los CVS dados en el ejercicio. Se usaron algunas Listas ya definidas y un Diccionario con datos generados por IA. Se establecen 40 productos y 180 Clientes.  <br>
Rango de fechas para las ventas va desde 1/2020 hasta el 10/2025. El mes de octubre de 2025 es el mes adicional (ventas_add.csv). 
<br>
Contenidos: <br>
• READ ME <br>
• orquestador.py – archivo de Python que al ser ejecutado corre todos los scripts de Python y SQL necesarios para crear y cargar el DataWarehause <br>
• SQLQuerySTAGING.sql – Script para crear las tablas STAGING <br>
• SQLQueryINT.sql – Script para crear las tablas INT <br>
• SQLQueryCreateDW.sql – Script para crear las tablas finales del DW <br>
• SQLQueryStoreProcedures.sql – Script para crear los StoreProcedures necesarios <br>
• extract_data.py – Script de Python para Extraer los datos de los archivos CSV y cargarlos en las tablas Staging <br>
• load_STG_to_INT.py – Script de Python para Cargar las tablas Int a partir las Staging <br>
• dw_loader.py - Script de Python para Cargar las tablas finales del DW a partir de las Int <br>
• config.ini – archivo con la configuración del servidor y la base de datos para que los scripts puedan acceder a ella  <br>
• Modelado.xlsx – archivo con el detalle del modelado del DW <br>
• DataShop_1.pbix – archivo de Power BI con el tablero para visualizar los datos  <br>
• Carpeta “DATASET” – carpeta que contiene los archivos CSV de donde los Scripts de Python toman los datos  <br>
• Carpeta “generar registros” – carpeta que contiene el Script de Python que se utilizo para generar los archivos CSV, al ejecutarlo los generan en esta carpeta  <br>
• Carpeta “SVG Backgrounds “ – carpeta con los archivos SVG para la construcción del Tablero en Power BI  <br>

<img width="1306" height="723" alt="Captura de pantalla (64)" src="https://github.com/user-attachments/assets/4b112376-b274-4293-8c6b-1c64a574f5b6" />

<img width="1308" height="723" alt="Captura de pantalla (65)" src="https://github.com/user-attachments/assets/e3c11614-644a-4bfb-8abc-045ede16c872" />

<img width="1318" height="725" alt="Captura de pantalla (66)" src="https://github.com/user-attachments/assets/cc6df001-a216-43cf-a8bf-071d99ffca1d" />

<img width="1301" height="703" alt="Captura de pantalla (67)" src="https://github.com/user-attachments/assets/3780b659-db96-4f73-80db-6a82efc5cf8f" />


<br><br>

El archivo orquestador corre todos los scripts de Python y SQL necesarios para extraer los datos y cargarlos en las tablas del DW en el siguiente orden: <br>
1. SQLQuerySTAGING.sql - Crea tablas STAGING <br>
2. SQLQueryINT.sql - Crea tablas INT <br>
3. SQLQueryCreateDW.sql - Crea tablas DW (Dimensiones y Fact) <br>
4. SQLQueryStoreProcedures.sql - Crea Stored Procedures <br>
5. extract_data.py - Extrae CSV -> STAGING <br>
6. load_STG_to_INT.py - Carga STAGING -> INT <br>
7. dw_loader.py - Carga INT -> DW <br>
<br>
La conexión al servidor y base de datos se maneja a partir de lo configurado en el Archivo  config.ini, que cada script de Python lee para poder conectarse a ella y hacer los cambios.<br>
<br>
Se establece una conexión pyodbc con control transaccional deshabilitado (autocommit = False) para asegurar la integridad de los datos. <br>
Para una mejor robustez se verifica con IF NOT EXISTS en los Scripts de SQL si existen los objetos en la base de datos antes de ser creados. Y de la misma forma CREATE OR ALTER PROCEDURE para los StoreProcedures. Y para asegurar la limpieza de la tabla de destino se usó el comando TRUNCATE TABLE. <br>
En la extracción de datos se mantiene un mapeo explícito de los nombres de los archivos CSV a las columnas esperadas, según los archivos dados de ejemplo en el ejercicio. <br>
En cada Script se verifica la existencia de los archivos necesarios, como la carpeta de DATASET en la extracción de datos, las tablas o los StoreProcedures antes de realizar una acción, y se advierte si no existen al usuario. Si ocurre un error fatal durante la carga, se ejecuta un ROLLBACK para revertir todas las inserciones pendientes y se cierra la conexión, evitando datos corruptos o incompletos. <br>
Se utilizaron la librerías de Python: Pandas, pyodbc, configparser, os, sys,  subprocess, y datetime. <br>
Cada tabla STAGING es vaciada y recargada completamente en cada ejecución, lo que asegura que el entorno de STAGING sea una copia fiel y limpia de las fuentes de datos. <br>
En el Script de transformación de datos existe un Análisis de Datos Problemáticos (check_ventas_problematic_data) : Llama a Stored Procedures de diagnóstico (sp_CheckVentasProblematicData, sp_GetVentasProblematicExamples) para identificar y reportar datos sucios o inválidos (ej. cantidades, precios o fechas incorrectas) en las tablas STAGING. <br>
La conexión con la base de datos se cierra automáticamente al terminar la carga. <br>
Durante la codificación surgieron errores de lectura de datos por diferentes detalles, como por ejemplo un encoding diferente de los archivos, y se agregaron funciones para advertir o solucionar estos problemas. 
<br><br>
Para la creación del informe interactivo en Power BI:  se usó direct Query para la conexión con la base de Datos. Siguió la creación de los gráficos detallados en la consigna de la Fase 3. Se genero una tabla de Medidas en Power BI para agrupar a todas las que fueron creadas para poder realizar las mediciones pedidas. <br>
Una vez completado y verificado el funcionamiento de los gráficos para cada Hoja se desarrolló el diseño: Se construyeron Fondos SVGs en Figma para mejorar el orden visual de los gráficos y mantener una coherencia del diseño. Se eligió una paleta de colores violetas, azules y grises teniendo en cuenta que se trata de una empresa que comercializa tecnología.<br>
