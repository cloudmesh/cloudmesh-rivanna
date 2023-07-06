from cloudmesh.common.Shell import Shell
from cloudmesh.common.console import Console
import os
from cloudmesh.common.FlatDict import FlatDict
from textwrap import dedent
import yaml
from cloudmesh.common.StopWatch import StopWatch

class Rivanna:


    def __init__(self, host="rivanna", debug=False):
        self.debug = debug
        self.data = dedent(
          """
          rivanna:
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
        if not self.debug:
             os.system(command)
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

    def create_singularity_image(self, name):
        """
        #! /bin/sh -x


        export SINGULARITY_CACHEDIR=/scratch/$USER/.singularity/
        export SINGULARITY_CACHEDIR=/$HOME/.singularity/
        mkdir -p $SINGULARITY_CACHEDIR
        NAME=$1

        start_total=`date +%s`
        cp ${NAME}.def build.def
        #sudo /opt/singularity/3.7.1/bin/singularity build output_image.sif build.def
        sudo singularity build output_image.sif build.def
        cp output_image.sif ${NAME}.sif
        # make -f Makefile clean
        end_total=`date +%s`
        time_total=$((end_total-start_total))
        echo "Time for image build: ${time_total} s"

        :param name:
        :type name:
        :return:
        :rtype:
        """

        try:
            cache = os.environ["SINGULARITY_CACHE"]
        except Exception as e:
            Console.error(e, traceflag=True)

        image = name.replace(".def", ".sif")
        StopWatch.start("build image")
        Shell.copy(name,  "build.def")
        os.system("sudo singularity build output_image.sif build.def")
        Shell.copy("output_image.sif",  image)
        StopWatch.stop("build image")

        timeer = StopWatch.get("build image")
        print ("Time to build", timeer)





