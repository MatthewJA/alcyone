"""Submit jobs to a cluster and poll for the result.

Matthew Alger
2019
"""

import inspect
import subprocess
import tempfile
import uuid


class Job:
    def __init__(self, function, user, server, remote_dir='~'):
        self.function = function
        self.user = user
        self.server = server
        self.remote_dir = remote_dir
        if not self.remote_dir.endswith('/'):
            self.remote_dir += '/'
        self.id = uuid.uuid4()  # uuid4 is random.
        self.out_filename = 'alcyone_out_{}.dat'.format(self.id)
        self.prefix = 'alcyone_in_{}'.format(self.id)
        self.suffix = '.py'

    def scp_to(self, local_path):
        target = '{}@{}:{}'.format(self.user, self.server, self.remote_dir)
        scp = subprocess.run(['scp', local_path, target])


    def run(self):
        source = inspect.getsource(self.function)
        assert source.startswith('def')

        runner = """
result = {}()
with open('{}', 'wb') as file:
    file.write(result)
""".format(self.function.__name__, self.out_filename)
        source = source + runner

        with tempfile.NamedTemporaryFile(
                mode='w+', prefix=self.prefix,
                suffix=self.suffix) as file:
            file.write(source)
            self.scp_to(file.name)


def test():
    return True

if __name__ == '__main__':
    Job(test, 'alger', 'miasma', '/tmp').run()
