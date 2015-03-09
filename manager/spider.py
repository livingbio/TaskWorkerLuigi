import luigi
from log2bq import Log2GsTask
PATH = "/srv/luigi/"

from fabric.api import *
from fabric_gce_tools import *
from fabric.context_managers import cd
import json

env.key_filename = '~/.ssh/google_compute_engine'
update_roles_gce()

@roles("spider")
def collect_item(filename):
    filename = filename.split('/')[-1]
    with cd("~/ec-spider"):
        run("python ggspider.py report --filename=%s"%filename)

    return filename + '.items.json'

@roles("dashboard")
def import_item(filename):
    with cd("~/dashboard"):
        run("python import_product.py %s" % filename)


class CollectItem(luigi.Task):
    hour_time = luigi.DateHourParameter()

    output_format = "{0.__class__.__name__}"

    def output(self):
        name = self.output_format.format(self)
        return luigi.LocalTarget(PATH + name)

    def requires(self):
        return [Log2GsTask(hour_time=self.hour_time)]

    def run(self):
        from fabric.tasks import execute

        for input in self.input():
            with input.open('r') as in_file:
                content = json.loads(in_file.read())
                filenames = content['output']['default']
                for filename in filenames:
                    item_filename = execute(collect_item, filename)

        with self.output().open('w') as out_file:
            out_file.write(item_filename)


class ImportItem(luigi.Task):
    hour_time = luigi.DateHourParameter()

    def requires(self):
        return [CollectItem(hour_time=self.hour_time)]

    output_format = "{0.__class__.__name__}"

    def output(self):
        name = self.output_format.format(self)
        return luigi.LocalTarget(PATH + name)

    def run(self):
        from fabric.tasks import execute

        for input in self.input():
            with input.open('r') as in_file:
                filename = in_file.read()
                execute(import_item, filename)

if __name__ == "__main__":
    luigi.run()
