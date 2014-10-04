from django.db import models 
import subprocess

class Task(models.Model):
    pid = models.CharField(max_length=255, null=True, blank=True)
    exit_code = models.IntegerField(null=True, blank=True)

    extra = models.TextField(null=True, blank=True)
    script = models.FileField(upload_to="scripts", null=True, blank=True)

    start_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    def start(self, redo=False):
        if self.pid and not redo:
            return self
        
        self.stop()
        file_path=self.script.path
        p = subprocess.Popen(['bash', file_path])
        self.pid = p.pid
        self.exit_code = None
        self.save()

        p.wait()
        self.exit_code = p.poll()
        self.save()

    def status(self):
        if not self.pid:
            return {"status": "failed", "msg": "not working"}
        
        elif self.pid and  self.exit_code == None:
            return {"status": "running"}

        else:
            if self.exit_code == 0:
                return {"status": "done", "msg": "success"}
            else:
                return {"status": "failed", "msg": "exit code {}".format(self.exit_code)}


    def stop(self):
        if not self.pid:
            return True
        
        p = subprocess.Popen(["kill", "-9", self.pid])
        if p:
            return False
        else:
            self.pid = None
            self.exit_code = None
            return True

