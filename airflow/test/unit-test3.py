import unittest
from airflow.utils.state import State

DEFAULT_DATE = '2019-10-03'
TEST_DAG_ID = 'test_my_custom_operator'

class MyCustomOperatorTest(unittest.TestCase):
   def setUp(self):
       self.dag = DAG(TEST_DAG_ID, schedule_interval='@daily', default_args={'start_date' : DEFAULT_DATE})
       self.op = MyCustomOperator(
           dag=self.dag,
           task_id='test',
           prefix='s3://bucket/some/prefix',
       )
       self.ti = TaskInstance(task=self.op, execution_date=DEFAULT_DATE)

   def test_execute_no_trigger(self):
       self.ti.run(ignore_ti_state=True)
       self.assertEqual(self.ti.state, State.SUCCESS)
       #Assert something related to tasks results
