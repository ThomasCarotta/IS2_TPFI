import uuid  # Módulo para generar identificadores únicos
from datetime import datetime  # Módulo para trabajar con fechas y horas
import boto3  # SDK de AWS para Python que permite interactuar con servicios de AWS
from botocore.exceptions import BotoCoreError, ClientError  # Excepciones específicas de AWS
import logging  # Módulo para gestionar mensajes de error y de seguimiento
from SingletonMeta import SingletonMeta  # Implementación de un patrón Singleton

class CorporateLog(metaclass=SingletonMeta):
    def __init__(self):
        # Inicializa el identificador único de la CPU utilizando la dirección MAC del dispositivo
        self.CPUid = str(uuid.getnode())
        # Conexión a DynamoDB usando boto3 y selecciona la tabla 'CorporateLog'
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table('CorporateLog')

    @staticmethod
    def getInstance():
        # Método estático para obtener la instancia única de CorporateLog
        return CorporateLog()
    
    def post(self, sessionid):
        """
        Registra una nueva entrada en la tabla CorporateLog de DynamoDB.
        
        Parámetros:
        - sessionid: Identificador de sesión.
        
        Retorna:
        - Mensaje de confirmación o error.
        """
        logging.debug(f"Registrando acción en CorporateLog con session ID {sessionid}")
        try:
            # Genera un ID único para el registro
            uniqueID = str(uuid.uuid4())
            # Obtiene la fecha y hora actual en formato ISO 8601
            ts = datetime.now().isoformat()
            # Inserta un nuevo registro en la tabla de DynamoDB con el ID único, la CPU ID, el session ID, y la marca de tiempo
            response = self.table.put_item(
                Item={
                    'id': uniqueID,
                    'CPUid': self.CPUid,
                    'sessionid': sessionid,
                    'timestamp': ts
                }
            )
            return "Registro guardado correctamente en DynamoDB."
        except Exception as e:
            # Maneja errores generales y los registra en el log
            logging.error(f"Error en post de CorporateLog: {e}")
            return f"Error al guardar el registro en DynamoDB: {e}"

    def list(self):
        """
        Lista los registros en la tabla CorporateLog filtrados por la CPU ID actual.
        
        Retorna:
        - Lista de registros o un mensaje de error.
        """
        try:
            # Escanea la tabla buscando registros donde el CPU ID coincide con el ID de la CPU actual
            response = self.table.scan(
                FilterExpression="CPUid = :CPUid",
                ExpressionAttributeValues={":CPUid": self.CPUid}
            )
            # Extrae los elementos obtenidos de la respuesta
            logs = response.get('Items', [])
            # Retorna los registros encontrados o un mensaje si no hay registros
            return logs if logs else "No se encontraron registros para la CPU especificada."
        except (BotoCoreError, ClientError) as error:
            # Maneja errores específicos de AWS
            return f"Error al listar los registros en DynamoDB: {error}"
