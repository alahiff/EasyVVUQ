"""Provides an action element to execute a simulation using PROMINENCE
and retrieve the output. The successful use of this actions
requires that two environment variables are setup:

- PROMINENCE_URL specifying the URL of the PROMINENCE REST API
- PROMINENCE_TOKEN specifying an access token

The input files are passed to the jobs using the PROMINENCE input
files mechanism which limits the size of the files. Output from
the simulation is retrieved using the PROMINENCE log mechanism.
Therefore the simulation output needs to be printed to stdout in 
the job. Again, if the simulation produces complicated or large output
you should extract the quantitities of interest on the Pod using some
kind of script and print them to stdout.

There are ways around this in PROMINENCE but these have not yet
been tried.
"""

import base64
import json
import os
import logging
import requests
import copy
from . import BaseAction

__license__ = "LGPL"

logger = logging.getLogger(__name__)


class ActionStatusProminence():
    """Provides a way to track the status of an on-going PROMINENCE
    action.

    Parameters
    ----------
    headers : dict of strings
        will be used to authenticate to the PROMINENCE server
    id : int
        job identifier
    input_files : list of str
        list of input files
    outfile : str
        a filename to write the output of the simulation
    """

    def __init__(self, headers, body, file_names, outfile):
        self.headers = headers
        self.body = dict(body)
        self.file_names = file_names
        self.outfile = outfile
        self._succeeded = False
        self._started = False
        self.id = None

    def start(self):
        """Will create the Prominence job and hence start the action.
        """
        if self.started():
            raise RuntimeError('The job has already started!')
        self.body['inputs'] = self.add_input_files(self.body, self.file_names)
        self.id = 0
        response = requests.post('%s/jobs' % os.environ['PROMINENCE_URL'], json=self.body, headers=self.headers)
        if response.status_code == 201:
            self._started = True
            if 'id' in response.json():
                self.id = response.json()['id']

    def started(self):
        """Will return true if start() was called.
        """
        return self._started

    def finished(self):
        """Will return True if the job has finished, otherwise will return False.
        """
        response = requests.get('%s/jobs/%d?all=true' % (os.environ['PROMINENCE_URL'], self.id), headers=self.headers)
        if 'status' in response.json()[0]:
            if response.json()[0]['status'] == 'completed':
                self._succeeded = True
            if response.json()[0]['status'] in ('pending', 'running'):
                return False
            else:
                return True
        else:
            return False

    def finalise(self):
        """Will read the std output from the job output it to a file
        """
        if not (self.finished() and self.succeeded()):
            raise RuntimeError("Cannot finalise an Action that hasn't finished.")
        response = requests.get('%s/jobs/%d/stdout' % (os.environ['PROMINENCE_URL'], self.id), headers=self.headers)
        with open(self.outfile, 'w') as fd:
            fd.write(response.text)

    def succeeded(self):
        """Will return True if the job has finished successfully, otherwise will return False.
        If the job hasn't finished yet will return False.
        """
        return self._succeeded

    def add_input_files(self, job, file_names):
        """Add input files to the job description
        """
        inputs = []
        for filename in file_names:
            with open(filename, 'rb') as fd:
                inputs.append({'filename':os.path.basename(filename),
                               'content':base64.b64encode(fd.read()).decode("utf-8")})
        return inputs

class ExecuteProminence(BaseAction):
    """ Provides an action element to run a shell command in a specified
    directory.

    Parameters
    ----------

    pod_config : str
        Filename of the JSON file with the PROMINENCE job configuration.
    input_file_names : list of str
        A list of input file names for your simulation.
    output_file_name : str
        An output file name for the output of the simulation.
    """

    def __init__(self, job_config, input_file_names, output_file_name):
        if os.name == 'nt':
            msg = ('Local execution is provided for testing on Posix systems'
                   'only. We detect you are using Windows.')
            logger.error(msg)
            raise NotImplementedError(msg)
        with open(job_config, 'r') as fd:
            self.dep = json.load(fd)
        self.input_file_names = input_file_names
        self.output_file_name = output_file_name
        self.headers = {'Authorization':'token %s' % os.environ['PROMINENCE_TOKEN']}

    def act_on_dir(self, target_dir):
        """Executes a containerized simulation on input files found in `target_dir`.

        target_dir : str
            Directory in which to execute simulation.
        """
        file_names = [os.path.join(target_dir, input_file_name) for input_file_name in self.input_file_names]
        dep = copy.deepcopy(self.dep)
        dep['name'] = target_dir
        return ActionStatusProminence(self.headers, dep, file_names, os.path.join(target_dir, self.output_file_name))
