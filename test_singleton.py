import unittest
from CorporateData import CorporateData
from CorporateLog import CorporateLog
from InterfazAWS import InterfazAWS

class TestSingletonPattern(unittest.TestCase):
    def test_singleton_corporate_data(self):
        instance1 = CorporateData.getInstance()
        instance2 = CorporateData.getInstance()
        self.assertIs(instance1, instance2, "CorporateData no es un Singleton")

    def test_singleton_corporate_log(self):
        instance1 = CorporateLog.getInstance()
        instance2 = CorporateLog.getInstance()
        self.assertIs(instance1, instance2, "CorporateLog no es un Singleton")

class TestInterfazAWS(unittest.TestCase):
    def setUp(self):
        self.session_id = "test_session"
        self.cpu_id = "test_cpu"
        self.interfaz = InterfazAWS(self.session_id, self.cpu_id)

    def test_valid_access(self):
        response = self.interfaz.consultar_datos_sede("UADER-FCyT-IS2")
        self.assertIn("datos_sede", response, "Error en el acceso correcto a datos de sede")

    def test_invalid_key_access(self):
        response = self.interfaz.consultar_datos_sede("clave_incorrecta")
        self.assertIn("error", response, "No se manejó correctamente el acceso con clave incorrecta")

    def test_missing_arguments(self):
        response = self.interfaz.consultar_datos_sede("")
        self.assertIn("error", response, "No se manejó correctamente el acceso con argumentos faltantes")

if __name__ == "__main__":
    unittest.main()
