#!/bin/bash

DIRECTORIO=Github/Proyecto_BI

if [ ! -d "$DIRECTORIO" ]
then

cd Github/ ; git clone https://github.com/camilosky6/Proyecto_BI.git ; chmod 755 Proyecto_BI ; chmod 755 Proyecto_BI/init.py ; pip3 install -r Proyecto_BI/requirements.txt 

echo "el proyecto  NO existe $DIRECTORIO se clono exitosamente"
else 

echo "el directorio  existe $DIRECTORIO, no se puede clonar "


fi
