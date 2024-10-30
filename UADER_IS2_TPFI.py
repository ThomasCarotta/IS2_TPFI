import uuid
from InterfazAWS import InterfazAWS
import logging

# Configuración de logging para errores críticos.
logging.basicConfig(level=logging.ERROR) 

def main():
    # Datos para consultas
    config_data = {
        "session_id": str(uuid.uuid4()),
        "cpu_id": str(uuid.getnode()),
        "id": "UADER-FCyT-IS2",
    }

    # Crear instancia de InterfazAWS
    interfaz = InterfazAWS(config_data["session_id"], config_data["cpu_id"])

    # Consultar datos de la sede en CorporateData
    print("\nConsultando datos de la sede en CorporateData...")
    print(interfaz.consultar_datos_sede(config_data["id"]))

    # Consultar CUIT de la sede en CorporateData
    print("\nConsultando el CUIT de la sede en CorporateData...")
    print(interfaz.consultar_cuit(config_data["id"]))

if __name__ == "__main__":
    main()
