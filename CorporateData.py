import boto3  # SDK de AWS para Python que permite interactuar con servicios de AWS
from botocore.exceptions import BotoCoreError, ClientError  # Excepciones específicas de AWS
import logging  # Módulo para gestionar mensajes de error y de seguimiento
import json  # Módulo para trabajar con datos JSON
from SingletonMeta import SingletonMeta  # Implementación de un patrón Singleton

class CorporateData(metaclass=SingletonMeta):
    """Clase que maneja los datos corporativos con implementación Singleton."""
    
    def __init__(self):
        # Conexión a DynamoDB usando boto3 y selecciona la tabla 'CorporateData'
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table('CorporateData')
    
    @staticmethod
    def getInstance():
        """Método estático para obtener la instancia única de CorporateData."""
        return CorporateData()

    def getData(self, uuid, uuidCPU, id):
        """
        Retorna información de la sede.
        
        Parámetros:
        - uuid: Identificador de sesión.
        - uuidCPU: Identificador de CPU.
        - id: Identificador de la sede.
        
        Retorna:
        - JSON con los datos de la sede o un mensaje de error.
        """
        # Mensaje de seguimiento para registrar la búsqueda de datos
        logging.debug(f"getData: Buscando datos para ID de sede {id} con session ID {uuid} y CPU ID {uuidCPU}")
        try:
            # Obtiene el elemento de la tabla usando el 'id' proporcionado
            response = self.table.get_item(Key={'id': id})
            if 'Item' in response:  # Verifica si el elemento fue encontrado
                return {
                    "ID": response['Item'].get("id"),
                    "Domicilio": response['Item'].get("domicilio"),
                    "Localidad": response['Item'].get("localidad"),
                    "CodigoPostal": response['Item'].get("cp"),
                    "Provincia": response['Item'].get("provincia")
                }
            else:
                return {"error": "Registro no encontrado"}  # Error si no encuentra el registro
        except Exception as e:
            logging.error(f"Error en getData: {e}")  # Registro del error
            return {"error": f"Error al acceder a la base de datos: {e}"}  # Devuelve un mensaje de error

    def getCUIT(self, uuid, uuidCPU, id):
        """
        Retorna el CUIT de la sede.
        
        Parámetros:
        - uuid: Identificador de sesión.
        - uuidCPU: Identificador de CPU.
        - id: Identificador de la sede.
        
        Retorna:
        - JSON con el CUIT o un mensaje de error.
        """
        try:
            # Consulta en la tabla para obtener el CUIT de la sede
            response = self.table.get_item(Key={'id': id})
            if 'Item' in response:  # Verifica si se encontró el registro
                return {"CUIT": response['Item'].get("CUIT")}
            else:
                return {"error": "Registro no encontrado"}  # Mensaje de error si no hay registro
        except (BotoCoreError, ClientError) as error:
            return {"error": f"Error al acceder a la base de datos: {error}"}  # Manejo de excepciones específicas de AWS

    def getSeqID(self, uuid, uuidCPU, id):
        """
        Retorna un identificador de secuencia único y lo incrementa en la base de datos.
        
        Parámetros:
        - uuid: Identificador de sesión.
        - uuidCPU: Identificador de CPU.
        - id: Identificador de la sede.
        
        Retorna:
        - JSON con el identificador de secuencia o un mensaje de error.
        """
        try:
            # Obtiene el registro para actualizar el identificador de secuencia
            response = self.table.get_item(Key={'id': id})
            if 'Item' in response:  # Verifica si el registro fue encontrado
                idSeq = response['Item'].get("idreq", 0) + 1  # Incrementa el idreq actual en 1
                # Actualiza el valor de idreq en la base de datos con el nuevo valor
                self.table.update_item(
                    Key={'id': id},
                    UpdateExpression="set idreq = :val",
                    ExpressionAttributeValues={':val': idSeq}
                )
                return {"idSeq": idSeq}  # Devuelve el nuevo valor de idreq
            else:
                return {"error": "Registro no encontrado"}  # Mensaje de error si no encuentra el registro
        except (BotoCoreError, ClientError) as error:
            return {"error": f"Error al acceder a la base de datos: {error}"}  # Manejo de excepciones específicas de AWS
