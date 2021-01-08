from airflow.models import DagBag
import unittest

class TestMyDAG(unittest.TestCase):
   @classmethod
   def setUpClass(cls):
       cls.dagbag = DagBag()

   def test_dag_loaded(self):
       dag = self.dagbag.get_dag(dag_id='my_dag')
       self.assertDictEqual(self.dagbag.import_errors, {})
       self.assertIsNotNone(dag)
       self.assertEqual(len(dag.tasks), 1)
