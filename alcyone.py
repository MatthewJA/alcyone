"""Submit jobs to a cluster and poll for the result.

Matthew Alger
2019
"""

import inspect
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import uuid


SLURM_TEMPLATE = """#!/bin/sh
# SLURM directives
#
#SBATCH --job-name=alcyone-{uid}
#SBATCH --time=10:00:00
#SBATCH --tasks-per-node=4
#SBATCH --gres=gpu:1
#SBATCH --mem=10g

module load hdf5
module load gcc
module load cuda/9.0.176
module load nccl/2.1.15-cuda90
module load pytorch/0.4.0-py36-cuda90
module load cudnn/v7.1.3-cuda90

{python_path} -u {script_path}
"""


def parse_fw_row(line, widths):
    total = 0
    row = []
    for width in widths:
        entry = line[total:total + width]
        total += width
        row.append(entry.strip())
    return row


def parse_fw(string):
    lines = string.split('\n')
    widths = [len(i) + 1 for i in lines[1].split(' ')]
    columns = parse_fw_row(lines[0], widths)
    result = []
    for row in lines[2:]:
        row = dict(zip(columns, parse_fw_row(row, widths)))
        result.append(row)
    return result


class Job:
    def __init__(self, function, user, server, python_path, remote_dir='~'):
        self.function = function
        self.user = user
        self.server = server
        self.remote_dir = Path(remote_dir)
        self.id = uuid.uuid4()  # uuid4 is random.
        self.out_filename = 'alcyone_out_{}.dat'.format(self.id)
        self.prefix = 'alcyone_in_{}'.format(self.id)
        self.suffix = '.py'
        self.remote_name = 'alcyone_in_{}.py'.format(self.id)
        self.python_path = Path(python_path)
        self.job_id = None
        self.timeout_sec = 60 * 3  # 3 minutes
        self.poll_delay = 5
        self.write_delay = 10

    def scp_to(self, local_path, remote_name):
        target = '{}@{}:{}'.format(self.user, self.server, self.remote_dir / remote_name)
        scp = subprocess.run(['scp', local_path, target])

    def scp_from(self, remote_path, local_path):
        target = '{}@{}:{}'.format(self.user, self.server, remote_path)
        print('Copying from', target, 'to', local_path)
        scp = subprocess.run(['scp', target, local_path])

    def ssh(self, script):
        remote = '{}@{}'.format(self.user, self.server)
        proc = subprocess.run(['ssh', remote], input=script.encode('ascii'),
                              stdout=subprocess.PIPE)
        stdout = proc.stdout.decode('ascii')
        return stdout

    def submit_job(self):
        slurm_remote_input_name = self.remote_name + '.slurm'
        slurm_remote_out_path = self.remote_dir / (self.remote_name + '.txt')
        with tempfile.NamedTemporaryFile(mode='w+') as slurm_file:
            slurm_script = SLURM_TEMPLATE.format(
                uid=self.id,
                python_path=self.python_path,
                script_path=self.remote_dir / self.remote_name)
            slurm_file.write(slurm_script)
            slurm_file.flush()
            print('Copying slurm script to', slurm_remote_input_name)
            self.scp_to(slurm_file.name, slurm_remote_input_name)

        submit_script = 'sbatch {}'.format(
            self.remote_dir / slurm_remote_input_name)  # TODO: Set an output path. -o doesn't work?
        print('Submitting:', submit_script)
        stdout = self.ssh(submit_script)
        match = re.match(r'Submitted batch job (\d+)', stdout)
        if not match:
            raise RuntimeError('Batch submission failed with: {}'.format(stdout))
        job_id = match.group(1)
        self.job_id = job_id
        print('Job ID:', job_id)

    def poll_job(self):
        assert self.job_id is not None
        poll_command = 'sacct -j {}'.format(self.job_id)
        for _ in range(self.timeout_sec // self.poll_delay):
            stdout = self.ssh(poll_command)
            result = parse_fw(stdout)
            result = [i for i in result if str(i['JobID']) == str(self.job_id)]
            if len(result) > 0:
                break
            time.sleep(self.poll_delay)
        result = result[-1]
        return result

    def run(self):
        source = inspect.getsource(self.function)
        assert source.startswith('def')

        runner = """
result = {}()
with open('{}', 'wb') as file:
    file.write(result)
""".format(self.function.__name__, self.remote_dir / self.out_filename)
        source = source + runner

        with tempfile.NamedTemporaryFile(
                mode='w+', prefix=self.prefix,
                suffix=self.suffix) as file:
            file.write(source)
            file.flush()
            self.scp_to(file.name, self.remote_name)
        self.submit_job()
        result = self.poll_job()


        with tempfile.TemporaryDirectory() as tempdir:
            local_name = 'alcyone-{}-out.dat'.format(self.id)
            local_path = Path(tempdir) / local_name
            time.sleep(self.write_delay)
            self.scp_from(Path('/home/') / self.user / 'slurm-{}.out'.format(self.job_id), local_path)
            with open(local_path) as f:
                return f.read()


def test():
    import sys
    print('Printing! Hello!')
    sys.stdout.flush()
    return b'Hello world!'


if __name__ == '__main__':
    test_job = Job(
        test,
        'alger',
        'miasma',
        '/home/alger/miniconda3/bin/python3',
        remote_dir='/tmp/')

    print(test_job.run())
