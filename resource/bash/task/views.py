# Create your views here.
import uuid
import os
from threading import Thread
from models import Task
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import json

@csrf_exempt
def start(request):
    if request.method == 'GET':
        template = '''
<html>
    <body>
        <form action="" method="POST" enctype="multipart/form-data">
        upload: <input type="file" name="script"><br>
        <input type="submit" name="submit" value="upload">
        </form>
    </body>
</html>
        '''
    
        return HttpResponse(template)

    else: 
        f = request.FILES.get('script')
        debug = request.GET.get('debug', False)

        # open task
        task = Task()
        task.script = f
        task.save()

        if debug:
            return task.start()
        else:
            t = Thread(target=task.start, args=[])
            t.start()

        result = json.dumps({"id": task.pk})
        return HttpResponse(result)


def stop(request):
    task_id = int(request.GET.get('id'))
    
    task = Task.objects.get(pk=int(task_id))
    task.stop()

    result = json.dumps({"task_id": task.pk, "success": True})
    return HttpResponse(result)


def status(request):
    task_id = int(request.GET.get('id'))
    
    task = Task.objects.get(pk=task_id)
    status = task.status()

    result = json.dumps(status)
    return HttpResponse(result)
