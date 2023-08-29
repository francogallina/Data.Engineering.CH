# Proyecto final del curso de Data Engineering de Coderhouse
### Desarrollado por Franco Gallina

## Requerimientos
- Tener instalado Docker y Docker Compose

## Descripción
El código se encarga de extraer diariamente los precios (apertura, cierre, máximo y mínimo) y volumenes de las acciones que conforman el Indice Dow Jones mediante el uso de una API.
La API utilizada corresponde a https://twelvedata.com/.
El código es ejecutado diariamente mediante Airflow y almacenados en una base de datos Redshift.

## Forma de uso
1. Ejecute `docker-compose up` en la terminal. 
2. Ingrese `localhost:8080` en el navegador web que prefiera.
3. Ingrese `airflow` como usuario y contraseña.
4. Dirigase a la opción `Admin` y luego en `Variables`.
5. Dentro de esa sección ingrese la Apikey proporcionada por https://twelvedata.com/ y las credenciales de Redshift.
![Variables Screenshot](Config_variables.png "Variables Screenshot")
6. Ejecute el DAG `Precio_acciones_Dow_Jones`
7. Para terminar con el proceso ingrese `docker-compose stop` en la terminal.


