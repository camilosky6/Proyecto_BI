#!/bin/bash

DIRECTORIO= Proyecto_BI

if [ ! -d "$DIRECTORIO" ]
then

git clone https://github.com/camilosky6/Proyecto_BI.git 
chmod 755 -R Proyecto_BI 
pip3 install -r Proyecto_BI/requirements.txt 

echo "el proyecto  NO existe $DIRECTORIO se clono exitosamente"
else

echo "el directorio  existe $DIRECTORIO, no se puede clonar "


fi