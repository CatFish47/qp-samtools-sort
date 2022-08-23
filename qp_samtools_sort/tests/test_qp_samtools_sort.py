# -----------------------------------------------------------------------------
# Copyright (c) 2020--, The Qiita Development Team.
#
# Distributed under the terms of the BSD 3-clause License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------
from unittest import main
from qiita_client.testing import PluginTestCase
from os import remove, environ
from os.path import exists, isdir, join, dirname
from shutil import rmtree, copyfile
from tempfile import mkdtemp
from json import dumps
from itertools import zip_longest
from functools import partial

from qp_samtools_sort import plugin
from qp_samtools_sort.utils import plugin_details
from qp_samtools_sort.qp_samtools_sort import (
    _generate_commands, samtools_sort_to_array,
    SORT_CMD)


class SamtoolsSortTests(PluginTestCase):
    def setUp(self):
        plugin("https://localhost:21174", 'register', 'ignored')

        out_dir = mkdtemp()
        self.maxDiff = None
        self.out_dir = out_dir
        self.params = {'threads': 2}
        self._clean_up_files = []
        self._clean_up_files.append(out_dir)

    def tearDown(self):
        for fp in self._clean_up_files:
            if exists(fp):
                if isdir(fp):
                    rmtree(fp)
                else:
                    remove(fp)

    def test_generate_commands(self):
        params = {'nprocs': 2,
                  'out_dir': '/foo/bar/output'}

        unsorted_bams_gz = ['untrimmed1.unsorted.bam.gz', 'untrimmed2.unsorted.bam.gz',
                            'trimmed1.unsorted.bam.gz', 'trimmed2.unsorted.bam.gz']

        obs = _generate_commands(unsorted_bams_gz, params['nprocs'],
                                 params['out_dir'])
        cmd = SORT_CMD.format(
            nprocs=params['nprocs'], out_dir_a=params['out_dir'], out_dir_b=params['out_dir'])
        ecmds = []
        for bam_gz in unsorted_bams_gz:
            bam = bam_gz[:-3]
            ecmds.append(cmd % (bam_gz, bam, bam, bam_gz))
        eof = [(f'{params["out_dir"]}/{bam}', 'tgz')
               for bam in unsorted_bams_gz]
        self.assertCountEqual(obs[0], ecmds)
        self.assertCountEqual(obs[1], eof)

    def test_samtools_sort(self):
        # inserting new prep template
        prep_info_dict = {
            'SKB8.640193': {'run_prefix': 'CALM_SEP_001974_81'},
            'SKD8.640184': {'run_prefix': 'CALM_SEP_001974_82'}}
        data = {'prep_info': dumps(prep_info_dict),
                # magic #1 = testing study
                'study': 1,
                'data_type': 'Metagenomic'}
        pid = self.qclient.post('/apitest/prep_template/', data=data)['prep']

        print(f"pid: {pid}")

        # inserting artifacts
        in_dir = mkdtemp()
        self._clean_up_files.append(in_dir)

        ub_1 = join(
            in_dir, 'CALM_SEP_001974_81_S382_L002.trimmed.unsorted.bam.gz')
        ub_2 = join(
            in_dir, 'CALM_SEP_001974_82_S126_L001.trimmed.unsorted.bam.gz')
        source_dir = 'qp_samtools_sort/support_files/raw_data'
        copyfile(
            f'{source_dir}/CALM_SEP_001974_81_S382_L002.trimmed.unsorted.bam.gz', ub_1)
        copyfile(
            f'{source_dir}/CALM_SEP_001974_82_S126_L001.trimmed.unsorted.bam.gz', ub_2)

        data = {
            'filepaths': dumps([
                (ub_1, 'tgz'),
                (ub_2, 'tgz')]),
            'type': "BAM",
            'name': "Test artifact",
            'prep': pid}
        aid = self.qclient.post('/apitest/artifact/', data=data)['artifact']

        self.params['input'] = aid

        print(f"artifact id: {aid}")

        data = {'user': 'demo@microbio.me',
                'command': dumps([plugin_details['name'],
                                  plugin_details['version'],
                                  'Samtools sorting']),
                'status': 'running',
                'parameters': dumps(self.params)}
        job_id = self.qclient.post(
            '/apitest/processing_job/', data=data)['job']

        out_dir = mkdtemp()
        self._clean_up_files.append(out_dir)

        # adding extra parameters
        self.params['environment'] = environ["ENVIRONMENT"]

        # Get the artifact filepath information
        artifact_info = self.qclient.get("/qiita_db/artifacts/%s/" % aid)

        print(f"artifact info: {artifact_info}")

        # Get the artifact metadata
        prep_info = self.qclient.get('/qiita_db/prep_template/%s/' % pid)
        prep_file = prep_info['prep-file']

        url = 'this-is-my-url'

        main_qsub_fp, finish_qsub_fp, out_files_fp = samtools_sort_to_array(
            artifact_info['files'], out_dir, self.params, prep_file,
            url, job_id)

        od = partial(join, out_dir)
        self.assertEqual(od(f'{job_id}.qsub'), main_qsub_fp)
        self.assertEqual(od(f'{job_id}.finish.qsub'), finish_qsub_fp)
        self.assertEqual(od(f'{job_id}.out_files.tsv'), out_files_fp)

        with open(main_qsub_fp) as f:
            main_qsub = f.readlines()
        with open(finish_qsub_fp) as f:
            finish_qsub = f.readlines()
        with open(out_files_fp) as f:
            out_files = f.readlines()
        with open(f'{out_dir}/samtools_sort.array-details') as f:
            commands = f.readlines()

        exp_main_qsub = [
            '#!/bin/bash\n',
            '#PBS -M qiita.help@gmail.com\n',
            f'#PBS -N {job_id}\n',
            '#PBS -l nodes=1:ppn=2\n',
            '#PBS -l walltime=30:00:00\n',
            '#PBS -l mem=16g\n',
            f'#PBS -o {out_dir}/{job_id}_${{PBS_ARRAYID}}.log\n',
            f'#PBS -e {out_dir}/{job_id}_${{PBS_ARRAYID}}.err\n',
            '#PBS -t 1-2%8\n',
            '#PBS -l epilogue=/home/qiita/qiita-epilogue.sh\n',
            'set -e\n',
            f'cd {out_dir}\n',
            'source /home/runner/.profile; conda activate qp-samtools-sort\n',
            'date\n',
            'hostname\n',
            'echo ${PBS_JOBID} ${PBS_ARRAYID}\n',
            'offset=${PBS_ARRAYID}\n', 'step=$(( $offset - 0 ))\n',
            f'cmd=$(head -n $step {out_dir}/samtools_sort.array-details | '
            'tail -n 1)\n',
            'eval $cmd\n',
            'set +e\n',
            'date\n']
        self.assertEqual(main_qsub, exp_main_qsub)

        exp_finish_qsub = [
            '#!/bin/bash\n',
            '#PBS -M qiita.help@gmail.com\n',
            f'#PBS -N finish-{job_id}\n',
            '#PBS -l nodes=1:ppn=1\n',
            '#PBS -l walltime=10:00:00\n',
            '#PBS -l mem=10g\n',
            f'#PBS -o {out_dir}/finish-{job_id}.log\n',
            f'#PBS -e {out_dir}/finish-{job_id}.err\n',
            '#PBS -l epilogue=/home/qiita/qiita-epilogue.sh\n',
            'set -e\n',
            f'cd {out_dir}\n',
            'source /home/runner/.profile; conda activate qp-samtools-sort\n',
            'date\n',
            'hostname\n',
            'echo $PBS_JOBID\n',
            f'finish_qp_samtools_sort this-is-my-url {job_id} {out_dir}\n',
            'date\n']
        self.assertEqual(finish_qsub, exp_finish_qsub)

        exp_out_files = [
            f'{out_dir}/CALM_SEP_001974_81_S382_L002.trimmed.unsorted.bam.gz\ttgz\n',
            f'{out_dir}/CALM_SEP_001974_82_S126_L001.trimmed.unsorted.bam.gz\ttgz']
        self.assertEqual(out_files, exp_out_files)
        print(out_files)

        # the easiest to figure out the location of the artifact input files
        # is to check the first file of the raw forward reads
        apath = dirname(artifact_info['files']['tgz'][0])
        exp_commands = [
            f'gunzip {apath}/CALM_SEP_001974_81_S382_L002.trimmed.unsorted.bam.gz; '
            f'samtools sort {apath}/CALM_SEP_001974_81_S382_L002.trimmed.unsorted.bam '
            f'-o {out_dir}/CALM_SEP_001974_81_S382_L002.trimmed.unsorted.bam -@ 2; '
            f'gzip {out_dir}/CALM_SEP_001974_81_S382_L002.trimmed.unsorted.bam.gz',
            f'gunzip {apath}/CALM_SEP_001974_82_S126_L001.trimmed.unsorted.bam.gz; '
            f'samtools sort {apath}/CALM_SEP_001974_82_S126_L001.trimmed.unsorted.bam '
            f'-o {out_dir}/CALM_SEP_001974_82_S126_L001.trimmed.unsorted.bam -@ 2; '
            f'gzip {out_dir}/CALM_SEP_001974_82_S126_L001.trimmed.unsorted.bam.gz'
        ]
        self.assertEqual(commands, exp_commands)


if __name__ == '__main__':
    main()
