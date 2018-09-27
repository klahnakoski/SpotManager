from mo_logs.strings import quote

from mo_json import value2json

from mo_threads.python import Python


class FabricProcess(object):


    def __init__(self, name, fabric_settings):
        """
        :param name:
        :param fabric_settings:
        """
        self.instance = Python(name, config={})
        self.instance.import_module("fabric.context_managers", ["cd", "hide"])
        self.instance.import_module("fabric.contrib", ["files"])
        self.instance.import_module("fabric.operations", ["sudo", "run", "put", "get"])
        self.instance.import_module("fabric.state", ["env"])

        for k, v in fabric_settings:
            self.instance.execute_script("env["+quote(k)+"]="+value2json(v))


    def __getattr__(self, command):

        def _add_private_file(self):
            run('rm -f /home/ubuntu/private.json')
            put('~/private_active_data_etl.json', '/home/ubuntu/private.json')
            with cd("/home/ubuntu"):
                run("chmod o-r private.json")


    def cd(self, path):
        return Context(self.instance, )

        context = self.instance.cd(path)
        exit = self.instance.execute_script(context, "__enter__", [])





class Context(object):

    def __init__(self, proc, *args):
        self.python=proc
        proc.e
        self.var = var

    def __enter__(self):
        self.context = self.python.execute_script(self.var+".__enter__()")

    def __exit__(self):
        self.python.execute_script(self.context+".__exit__()")

