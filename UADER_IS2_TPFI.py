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
    print(interfaz.consultar_datos_sede(config_data["session_id"], config_data["cpu_id"], config_data["id"]))

    # Consultar CUIT de la sede en CorporateData
    print("\nConsultando el CUIT de la sede en CorporateData...")
    print(interfaz.consultar_cuit(config_data["session_id"], config_data["cpu_id"], config_data["id"]))

    # Generar un nuevo ID de secuencia en CorporateData
    print("\nGenerando un nuevo ID de secuencia en CorporateData...")
    print(interfaz.generar_id_secuencia(config_data["session_id"], config_data["cpu_id"], config_data["id"]))

    # Registrar log en CorporateLog
    print("\nRegistrando acción en CorporateLog...")
    print(interfaz.registrar_log())

    # Registros asociados al CPU
    print("\nCantidad de registros asociados al CPU actual:")
    print(interfaz.listar_logs(filtro="cpu"))

    # Contar registros asociados a la sesión actual
    print("\nCantidad de registros asociados a la sesión actual:")
    print(interfaz.listar_logs(filtro="session"))

if __name__ == "__main__":
    main()