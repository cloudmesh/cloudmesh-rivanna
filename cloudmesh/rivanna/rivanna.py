from cloudmesh.common.Shell import Shell
from cloudmesh.common.console import Console
import os
from cloudmesh.common.FlatDict import FlatDict
from textwrap import dedent
import yaml

class Rivanna:


    def __init__(self, host="rivanna"):
        self.data = dedent(
          """
          rivanna:
            v100:
              gpu: v100
              gres: "gpu:v100:1"
              partition: "bii-gpu"
              account: "bii_dsc_community"
            a100:
              gpu: a100
              gres: "gpu:a100:1"
              partition: "gpu"
              account: "bii_dsc_community"
            a100-localscratch:
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
            a100-pod:
              gres: "gpu:a100:1"
              account: "bii_dsc_community"
              alllocations: "superpodtest"
              constraint: "gpupod"
              partition: gpu
          greene:
            v100:
              gres: "gpu:v100:1"
            a100:
              gres: "gpu:a100:1"
        """
        )
        self.directive = yaml.safe_load(self.data)

    def parse_sbatch_parameter(self, parameters):
        result = {}
        data = parameters.split(",")
        for line in data:
            key, value = line.split(":",1)
            result[key] = value
        return result

    def directive_from_key(self, key):
        return self.directive[key]

    def create_slurm_directives(self, host=None, key=None):
        directives = self.directive[host][key]
        block = ""

        def create_direcitve(name):
            return f"#SBATCH --{name}={directives[name]}\n"

        for key in directives:
            block = block + create_direcitve(key)

        return block


    def login(self, host, key):
        """
        ssh on rivanna by executing an interactive job command

        :param gpu:
        :type gpu:
        :param memory:
        :type memory:
        :return:
        :rtype:
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
        # if not self.debug:
        #     os.system(command)
        return ""


    def cancel(self, job_id):
        """
        cancels the job with the given id

        :param job_id:
        :type job_id:
        :return:
        :rtype:
        """
        raise NotImplementedError

    def storage(self, directory=None):
        """
        get info about the directory

        :param directory:
        :type directory:
        :return:
        :rtype:
        """
        raise NotImplementedError

    def edit(self, filename=None, editor="emacs"):
        """
        start the commandline editor of choice on the file on rivanna in the current terminal

        :param filename:
        :type filename:
        :return:
        :rtype:
        """

    def browser(self, url):
        Shell.browser(filename=url)