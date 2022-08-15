# -----------------------------------------------------------------------------
# Copyright (c) 2020, Qiita development team.
#
# Distributed under the terms of the BSD 3-clause License License.
#
# The full license is in the file LICENSE, distributed with this software.
# -----------------------------------------------------------------------------

from qiita_client import QiitaPlugin, QiitaCommand
from .qp_samtools_sort import samtools_sort
from .utils import plugin_details
from os.path import splitext


THREADS = 15


# Initialize the plugin
plugin = QiitaPlugin(**plugin_details)

# Define the command
req_params = {'input': ('artifact', ['per_sample_BAM'])}
opt_params = {
    'threads': ['integer', f'{THREADS}']}

outputs = {'Filtered files': 'per_sample_BAM'}
default_params = {
    'default params': {'threads': THREADS}
}

samtools_sort_cmd = QiitaCommand(
    'Samtools sorting', "Sorting using samtools",
    samtools_sort, req_params, opt_params, outputs, default_params)

plugin.register_command(samtools_sort_cmd)
