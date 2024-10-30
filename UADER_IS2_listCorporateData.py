import boto3
import json
from decimal import Decimal

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)  # O usa int(obj) si todos los valores son enteros
    raise TypeError

def listar_corporate_data():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('CorporateData')

    try:
        response = table.scan()  # Escanea todos los elementos en la tabla
        data = response.get('Items', [])
        
        # Convertir todos los elementos en 'data' utilizando la función decimal_default
        return json.dumps({"corporate_data": data}, indent=4, default=decimal_default)
    except Exception as e:
        return json.dumps({"error": f"Error al acceder a la base de datos: {e}"})

if __name__ == "__main__":
    print(listar_corporate_data())
