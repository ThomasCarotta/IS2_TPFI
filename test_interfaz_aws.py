import unittest
from InterfazAWS import InterfazAWS
from CorporateData import CorporateData
from CorporateLog import CorporateLog
import uuid
import json

class TestInterfazAWS(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.session_id = str(uuid.uuid4())
        cls.cpu_id = str(uuid.getnode())
        cls.sede_id = "UADER-FCyT-IS2"
        cls.interfaz = InterfazAWS(cls.session_id, cls.cpu_id)

    def test_registrar_log(self):
        """Prueba registrar log en CorporateLog."""
        response = json.loads(self.interfaz.registrar_log())
        self.assertIn("resultado_registro", response)
        self.assertEqual(response["resultado_registro"], "Registro guardado correctamente en DynamoDB.")

    def test_consultar_datos_sede(self):
        """Prueba consultar datos de la sede en CorporateData."""
        response = json.loads(self.interfaz.consultar_datos_sede(self.sede_id))
        self.assertIn("datos_sede", response)
        self.assertIn("Domicilio", response["datos_sede"])

    def test_consultar_cuit(self):
        """Prueba consultar CUIT de la sede en CorporateData."""
        response = json.loads(self.interfaz.consultar_cuit(self.sede_id))
        self.assertIn("cuit", response)
        self.assertIn("CUIT", response["cuit"])

    def test_generar_id_secuencia(self):
        """Prueba generar un nuevo ID de secuencia en CorporateData."""
        response = json.loads(self.interfaz.generar_id_secuencia(self.sede_id))
        self.assertIn("nuevo_id_secuencia", response)
        self.assertIsInstance(response["nuevo_id_secuencia"], int)

    def test_listar_logs_cpu(self):
        """Prueba listar logs asociados al CPU actual en CorporateLog."""
        response = json.loads(self.interfaz.listar_logs(filtro="cpu"))
        self.assertIn("logs_por_cpu", response)
        self.assertIsInstance(response["logs_por_cpu"], list)

    def test_listar_logs_session(self):
        """Prueba listar logs asociados a la sesión actual en CorporateLog."""
        response = json.loads(self.interfaz.listar_logs(filtro="session"))
        self.assertIn("logs_por_sesion", response)
        self.assertIsInstance(response["logs_por_sesion"], list)

    def test_invalid_sede_id(self):
        """Prueba manejar un ID de sede inválido."""
        invalid_id = "INVALID_ID"
        response = json.loads(self.interfaz.consultar_datos_sede(invalid_id))
        self.assertIn("error", response.get("datos_sede", {}))  # Verifica "error" dentro de "datos_sede"


if __name__ == "__main__":
    unittest.main()
