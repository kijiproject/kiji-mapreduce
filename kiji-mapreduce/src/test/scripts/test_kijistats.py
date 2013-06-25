'''
Copyright 2013 WibiData, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
import os
import sys
import unittest
srcpath = os.path.abspath(os.getcwd() + '/../../main/scripts')
sys.path.insert(0, srcpath)
import kijistats
import tempfile

class TestKijiStats(unittest.TestCase):
    def setUp(self):
        self.input_dir = os.path.abspath(os.getcwd() + '/../resources/org/kiji/mapreduce/kijistats')

    def testArgsForJob(self):
        self.assertRaises(ValueError, kijistats.main, ['kijistats', '--to-file', 'xyz'])

    # By job name may contain different jobs of the same type
    def testByJobName(self):
        expectedOutput = "public synchronized org.kiji.schema.KijiSchemaTable.SchemaEntry " + \
            "org.kiji.schema.impl.HBaseSchemaTable.getSchemaEntry(org.kiji.schema.util.BytesKey)," + \
            " 278514701, 875, 709618.05"
        tf = tempfile.NamedTemporaryFile(delete=False)
        filename = tf.name
        kijistats.main(['./kijistats.py',
                        '--stats-dir',
                        self.input_dir,
                        '--by-jobname',
                        'TestingGatherer',
                        '--to-file',
                        filename])
        with open(filename) as f:
            for line in f:
                if "getSchemaEntry" in line:
                    self.assertEqual(expectedOutput, line.rstrip())
                    return
        self.fail("Expected line not found in output")

    # Aggregate functions
    def testByFunction(self):
        expectedOutput = "attempt_local1340085606_0004_m_000000_0, 157036000, 320, 490737.5"
        tf = tempfile.NamedTemporaryFile(delete=False)
        filename = tf.name
        kijistats.main(['./kijistats',
                        '--stats-dir',
                        self.input_dir,
                        '--by-function',
                        'getSchemaEntry',
                        '--to-file',
                        filename])
        with open(filename) as f:
            for line in f:
                if "attempt_local1340085606_0004_m_000000_0" in line:
                    self.assertEqual(expectedOutput, line.rstrip())
                    return
        self.fail("Expected line not found in output")

    def testByJobID(self):
        expectedOutput = "public synchronized org.kiji.schema.KijiSchemaTable.SchemaEntry " + \
            "org.kiji.schema.impl.HBaseSchemaTable.getSchemaEntry(org.kiji.schema.util.BytesKey)," + \
            " 157036000, 320, 490737.5"
        tf = tempfile.NamedTemporaryFile(delete=False)
        filename = tf.name
        kijistats.main(['./kijistats',
                        '--stats-dir',
                        self.input_dir,
                        '--by-job',
                        'job_local1340085606_0004',
                        '--to-file',
                        filename])
        with open(filename) as f:
            for line in f:
                if "getSchemaEntry" in line:
                    self.assertEqual(expectedOutput, line.rstrip())
                    return
        self.fail("Expected line not found in output")

suite = unittest.TestLoader().loadTestsFromTestCase(TestKijiStats)
unittest.TextTestRunner(verbosity=2).run(suite)
