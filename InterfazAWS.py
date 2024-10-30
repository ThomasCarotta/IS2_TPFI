import json  # Módulo para manejar JSON en Python
from CorporateLog import CorporateLog  # Importa la clase CorporateLog para registrar y consultar logs
from CorporateData import CorporateData  # Importa la clase CorporateData para acceder a datos de sedes

class InterfazAWS:
    def __init__(self, session_id, cpu_id):
        # Inicializa la clase con identificadores de sesión y CPU
        self.session_id = session_id
        self.cpu_id = cpu_id
        # Obtiene instancias únicas de CorporateLog y CorporateData usando el patrón Singleton
        self.log_instance = CorporateLog.getInstance()
        self.data_instance = CorporateData.getInstance()

    def registrar_log(self):
        # Llama al método post para registrar un log y retorna el resultado en formato JSON
        result = self.log_instance.post(self.session_id)
        return json.dumps({"resultado_registro": result})

    def consultar_datos_sede(self, session_id, cpu_id, sede_id):
        # Obtiene los datos de una sede usando el método getData de CorporateData
        data = self.data_instance.getData(session_id, cpu_id, sede_id)
        return json.dumps({"datos_sede": data})

    def consultar_cuit(self, session_id, cpu_id, sede_id):
        # Consulta el CUIT de una sede a través del método getCUIT de CorporateData
        cuit = self.data_instance.getCUIT(session_id, cpu_id, sede_id)
        return json.dumps({"cuit": cuit})

    def generar_id_secuencia(self, session_id, cpu_id, sede_id):
        # Genera un nuevo ID de secuencia llamando al método getSeqID de CorporateData
        new_seq_id = self.data_instance.getSeqID(session_id, cpu_id, sede_id)
        # Convierte el ID de secuencia en un entero y lo retorna en formato JSON
        return json.dumps({"nuevo_id_secuencia": int(new_seq_id["idSeq"])})

    def listar_logs(self, filtro="cpu"):
        # Llama al método list para obtener todos los logs
        logs = self.log_instance.list()

        if filtro == "cpu":
            # Retorna los logs filtrados por CPU en formato JSON
            return json.dumps({"logs_por_cpu": logs}, indent=4)
        elif filtro == "session":
            # Filtra los logs por el ID de sesión actual y retorna el resultado en JSON
            logs_filtrados = [log for log in logs if log["sessionid"] == self.session_id]
            return json.dumps({"logs_por_sesion": logs_filtrados}, indent=4)
        else:
            # Maneja casos en los que el filtro es inválido
            return json.dumps({"error": "Filtro no válido. Use 'cpu' o 'session'."})
