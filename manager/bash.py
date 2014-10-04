#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright © 2014 vagrant 
#
# Distributed under terms of the MIT license.
from taskController import ExternalTaskController, TaskController
import os
import json
from datetime import timedelta, datetime
import luigi

# 將 deploy 檔案丟到server 上執行
class Test(TaskController):
    _task_url = 'http://localhost:1235'

    _files = {
        'script': open('delay')
    }

    output_format= "{0.__class__.__name__}"



luigi.run(main_task_cls=Test)
