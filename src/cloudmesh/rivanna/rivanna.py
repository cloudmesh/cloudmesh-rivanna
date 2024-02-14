from cloudmesh.common.Shell import Shell
from cloudmesh.common.console import Console
import os
from cloudmesh.common.FlatDict import FlatDict
from textwrap import dedent
import yaml
from cloudmesh.common.util import banner
from cloudmesh.common.StopWatch import StopWatch
import socket
import sys

class Rivanna:

    def jupyter(self, port=8000):
        """
        Start a Jupyter notebook on Rivanna.

        Args:
            port (int): The port number for Jupyter. Default is 8000.

        Note:
            This method assumes a VPN connection is active.
        """
        self.port = port

        # test if vpn is on

        # start login on machine and start jupyter
        "jupyter notebook --no-browser --port=<PORT>"
        # open tunnel
        "ssh -L 8080:localhost:<PORT> <REMOTE_USER>@<REMOTE_HOST>"
        # access to notebook on localhost

    def __init__(self, host="rivanna", debug=False):
        """
        Initialize the Rivanna class.

        Args:
            host (str): The hostname, default is "rivanna".
            debug (bool): Enable debugging if True, default is False.
        """
        self.debug = debug
        print ("HOST", host)
        self.data = dedent(
          """
          ubuntu:
            ubuntu:
              partition: "ubuntu"
              account: "ubuntu"
              gres: "none"
          macos:
            macos:
              partition: "macos"
              account: "macos"
              gres": "none"
          rivanna:
            parallel:
              partition: "parallel"
              account: "bii_dsc_community"
              nodes: 2
              ntask-per-node: 4
            v100:
              gres: "gpu:v100:1"
              partition: "bii-gpu"
              account: "bii_dsc_community"
            a100:
              gres: "gpu:a100:1"
              partition: "gpu"
              account: "bii_dsc_community"
            a100-dgx:
              gres: "gpu:a100:1"
              reservation: "bi_fox_dgx"
              partition: "bii-gpu"
              account: "bii_dsc_community"
            k80:
              gres: "gpu:k80:1"
              partition: "gpu"
              account: "bii_dsc_community"
            p100:
              gres: "gpu:p100:1"
              partition: "gpu"
              account: "bii_dsc_community"
            a6000:
              gres: "gpu:a6000:1"
              partition: "gpu"
              account: "bii_dsc_community"
            a100-pod:
              gres: "gpu:a100:1"
              account: "bii_dsc_community"
              constraint: "gpupod"
              partition: gpu
            rtx2080:
              gres: "gpu:rtx2080:1"
              partition: "gpu"
              account: "bii_dsc_community"
            rtx3090:
              gres: "gpu:rtx3090:1"
              partition: "gpu"
              account: "bii_dsc_community"          
          greene:
            v100:
              gres: "gpu:v100:1"
            a100:
              gres: "gpu:a100:1"
          maltlab:
            rtx_titan:
              gres: "gpu:rtx_titan:1"
          crusher:
            default:
              partition: "gen150_smartsim"          
          frontier:
            default:
              partition: "gen150_smartsim"          
        """
        )
        self.directive = yaml.safe_load(self.data)

    def parse_sbatch_parameter(self, parameters):
        """
        Parse the parameters string and convert it to a dictionary.

        Args:
            parameters (str): Comma-separated string of parameters.

        Returns:
            dict: A dictionary containing parsed parameters.
        """
        result = {}
        data = parameters.split(",")
        for line in data:
            key, value = line.split(":",1)
            result[key] = value
        return result

    def directive_from_key(self, key):
        """
        Retrieve the Slurm directives for a specific key.

        Args:
            key (str): The key to retrieve directives.

        Returns:
            dict: A dictionary containing Slurm directives for the specified key.
        """
        return self.directive[key]

    def create_slurm_directives(self, host=None, key=None):
        """
        Create Slurm directives based on the provided host and key.

        Args:
            host (str): The hostname.
            key (str): The key to retrieve directives.

        Returns:
            str: A string containing Slurm directives for the specified host and key.
        """
        try:
            directives = self.directive[host][key]
        except:
            Console.error(f"In directive searching for:\n  host {host}\n  key {key}\nNot found")
            sys.exit()
        block = ""

        def create_direcitve(name):
            return f"#SBATCH --{name}={directives[name]}\n"

        for key in directives:
            block = block + create_direcitve(key)

        return block


    def login(self, host, key):
        """
        SSH on Rivanna by executing an interactive job command.

        Args:
            host (str): The hostname.
            key (str): The key used for authentication.

        Returns:
            str: Empty string.
        """

        def create_parameters(host, key):

            directives = self.directive[host][key]
            block = ""

            def create_direcitve(name):
                return f" --{name}={directives[name]}"

            for key in directives:
                block = block + create_direcitve(key)

            return block


        parameters = create_parameters(host, key)
        command = f'ssh -tt {host} "/opt/rci/bin/ijob{parameters}"'

        Console.msg(command)
        if not self.debug:
             os.system(command)
        return ""


    def cancel(self, job_id):
        """
        Cancel the job with the given ID.

        Args:
            job_id (str): The ID of the job to be canceled.

        Raises:
            NotImplementedError: This method is not implemented.
        """
        raise NotImplementedError

    def storage(self, directory=None):
        """
        Get information about the specified directory.

        Args:
            directory (str): The directory to get information about.

        Raises:
            NotImplementedError: This method is not implemented.
        """
        raise NotImplementedError

    def edit(self, filename=None, editor="emacs"):
        """
        Start the command-line editor of choice on the file on Rivanna in the current terminal.

        Args:
            filename (str): The name of the file to be edited.
            editor (str): The command-line editor to use, default is "emacs".

        Returns:
            None
        """
        raise NotImplementedError

    def create_apptainer_image(self, name):
        """
        Create a apptainer image on Rivanna.

        Args:
            name (str): The name of the apptainer image.

        Returns:
            None

        Requires:
            export APPTAINER_CACHEDIR=/scratch/$USER/.apptainer/
            or
            export APPTAINER_CACHEDIR=/$HOME/.apptainer/

            Please note it is prefered to use scratch as the home directory may have too little storage

        """

        try:
            cache = os.environ["APPTAINER_CACHEDIR"]
            banner("Cloudmesh Rivanna Singularity Build")

            image = os.path.basename(name.replace(".def", ".sif"))

            print("Image name       :", image)
            print("Singularity cache:", cache)
            print("Definition       :", name)
            print()
            StopWatch.start("build image")
            os.system("apptainer build {image} {name}")
            StopWatch.stop("build image")
            size = Shell.run(f"du -sh {image}").split()[0]
            timer = StopWatch.get("build image")
            print()
            print(f"Time to build {image}s ({size}) {timer}s")
            print()

        except Exception as e:
            Console.error(e, traceflag=True)
            

