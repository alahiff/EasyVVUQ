"""Provides an action element to execute a simulation using PROMINENCE
and retrieve the output. The successful use of this action
requires that the environment variable PROMINENCE_URL exists, specifying
the URL of the PROMINENCE REST API and that the file ~/.prominence/token
contains JSON where "access_token" specifies a valid access token.

There are two options implemented for data access:
(1) EasyVVUQ and PROMINENCE jobs make use of the same shared filesystem,
making use of OneData or B2DROP for example. In this case nothing needs
to be done to transfer input files to jobs or retrieve output files. It
is important to ensure that B2DROP has the same mount point on the
host running EasyVVUQ and in the jobs.

(2) The input files are passed to the jobs using the PROMINENCE input
files mechanism which limits the size of the files. Output from
the simulation is retrieved using the PROMINENCE log mechanism.
Therefore the simulation output needs to be printed to stdout in 
the job. Again, if the simulation produces complicated or large output
you should extract the quantitities of interest on the Pod using some
kind of script and print them to stdout.
"""

import base64
import copy
import json
import os
import logging
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
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

    def __init__(self, url, body, file_names, outfile):
        self.url = url
        self.body = dict(body)
        self.file_names = file_names
        self.outfile = outfile
        self._succeeded = False
        self._started = False
        self.id = None

        retry_strategy = Retry(
            total=5,
            status_forcelist=[500],
            method_whitelist=["HEAD", "GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.http = requests.Session()
        self.http.mount("https://", adapter)

    def start(self):
        """Will create the Prominence job and hence start the action.
        """
        if self.started():
            raise RuntimeError('The job has already started!')
        if self.file_names:
            self.body['inputs'] = self.add_input_files(self.file_names)
        self.id = 0
        response = self.http.post('%s/jobs' % self.url, json=self.body, headers=self.create_header())
        if response.status_code == 201:
            self._started = True
            if 'id' in response.json():
                self.id = response.json()['id']
        else:
            raise RuntimeError('Failed to submit job: %s' % response.text)

    def started(self):
        """Will return true if start() was called.
        """
        return self._started

    def finished(self):
        """Will return True if the job has finished, otherwise will return False.
        """
        response = self.http.get('%s/jobs/%d?all=true' % (self.url, self.id), headers=self.create_header())
        if response.status_code != 200:
            raise RuntimeError('Got error checking status of job: %s' % response.text)

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
        """Will read the std output from the job and write it to a file
        """
        if not (self.finished() and self.succeeded()):
            raise RuntimeError("Cannot finalise an Action that hasn't finished.")
        if self.outfile:
            response = self.http.get('%s/jobs/%d/stdout' % (self.url, self.id), headers=self.create_header())
            with open(self.outfile, 'w') as fd:
                fd.write(response.text)

    def succeeded(self):
        """Will return True if the job has finished successfully, otherwise will return False.
        If the job hasn't finished yet will return False.
        """
        return self._succeeded

    def add_input_files(self, file_names):
        """Add input files to the job description
        """
        inputs = []
        for filename in file_names:
            with open(filename, 'rb') as fd:
                inputs.append({'filename':os.path.basename(filename),
                               'content':base64.b64encode(fd.read()).decode("utf-8")})
        return inputs

    def create_header(self):
        """Create the Authorization header. We do this for every request because the
        token may have been updated.
        """
        if os.path.isfile(os.path.expanduser('~/.prominence/token')):
            with open(os.path.expanduser('~/.prominence/token')) as json_data:
                data = json.load(json_data)

            if 'access_token' in data:
                return {'Authorization':'token %s' % data['access_token']}

        raise RuntimeError("Unable to obtain access token")

class ExecuteProminence(BaseAction):
    """ Provides an action element to run a shell command in a specified
    directory.

    Parameters
    ----------

    job_config : str
        Filename of the JSON file with the PROMINENCE job configuration.
    input_file_names : list of str
        A list of input file names for your simulation.
    output_file_name : str
        An output file name for the output of the simulation.
    """

    def __init__(self, job_config, input_file_names=[], output_file_name=None):
        if os.name == 'nt':
            msg = ('Local execution is provided for testing on Posix systems'
                   'only. We detect you are using Windows.')
            logger.error(msg)
            raise NotImplementedError(msg)
        with open(job_config, 'r') as fd:
            self.dep = json.load(fd)
        self.input_file_names = input_file_names
        self.output_file_name = output_file_name
        self.url = os.environ['PROMINENCE_URL']

    def act_on_dir(self, target_dir):
        """Executes a containerized simulation on input files found in `target_dir`.

        target_dir : str
            Directory in which to execute simulation.
        """
        file_names = [os.path.join(target_dir, input_file_name)
                      for input_file_name in self.input_file_names]
        dep = copy.deepcopy(self.dep)
        dep['name'] = target_dir
        tasks = []
        for task in dep['tasks']:
            task['workdir'] = target_dir
            tasks.append(task)
        dep['tasks'] = tasks

        output_file_name = None
        if self.output_file_name:
            output_file_name = os.path.join(target_dir, self.output_file_name)

        return ActionStatusProminence(self.url, dep, file_names, output_file_name)
