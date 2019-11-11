import copy
import glob
import json
import os
import operator

from celery import Celery, signature, chain, Task, group, canvas

from config import config

operators = {
    '$eq': operator.eq
}


class JanusTask(Task):
    def run(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return super(JanusTask, self).__call__(*args, **kwargs)

    def insert_to_chain(self, task):
        if self.request.chain is None:
            self.request.chain = list()

        if type(task) == group:
            current_chain = copy.deepcopy(self.request.chain)
            current_chain.reverse()
            reorder = [signature(t) for t in current_chain]
            next_chain = chain(task, *reorder)
            self.request.chain = [dict(next_chain), ]
        elif type(task) == chain or type(task) == canvas._chain:
            unchain_tasks = task.unchain_tasks()
            unchain_tasks.reverse()
            print(f'unchain_tasks: {unchain_tasks}')
            self.request.chain.extend(unchain_tasks)
        elif type(task) == Task:
            self.request.chain.append(task.s())
        else:
            print(f'other {type(task)}')

    def get_base_extra(self):
        return dict(task_id=self.request.id, process_id=self.request.root_id,
                    parent_id=self.request.parent_id,
                    task_name=self.name)

    @classmethod
    def group_to_chord(cls, task):
        return task

    @classmethod
    def check_conditions(cls, target, statements):
        for op, statement in statements.items():
            if not operators[op](target, statement):
                return False
        return True


class EventTask(JanusTask):

    def __call__(self, *args, **kwargs):
        print(f'call {self.name}: {kwargs} {args}')
        return super(EventTask, self).__call__(*args, **kwargs)

    def on_success(self, retval, task_id, args, kwargs):
        pass

    def on_failure(self, exc, task_id=None, args=None, kwargs=None, einfo=None):
        pass

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        pass

    def apply_async(self, args=None, kwargs=None, task_id=None, producer=None,
                    link=None, link_error=None, shadow=None, **options):
        condition = options.get('condition')
        if condition:
            payload = args[0]
            for target, statements in condition.items():
                result = self.check_conditions(payload[target], statements) if target in payload else True
                if not result:
                    raise ValueError(f'Check {target} failed.')

        return super(EventTask, self).apply_async(args, kwargs, task_id, producer,
                                                  link, link_error, shadow, **options)

    @property
    def description(self):
        return self.task_name.replace("_", " ")

    @property
    def task_name(self):
        return self.name[self.name.rfind('.') + 1:]


class EntryTask(EventTask):
    def __call__(self, *args, **kwargs):
        return super(EntryTask, self).__call__(*args, **kwargs)


class FlowBuilder:
    flow_map = dict()

    # def __init__(self, flows: dict, flow_definitions: dict):

    @classmethod
    def init(cls, root_dir: str):
        files = [f for f in glob.glob(f"{root_dir}/data/flow.json", recursive=True)]
        for f in files:
            with open(f, 'r') as stream:
                flow_config = json.loads(stream.read())
            cls.parse_tasks(flow_config['tasks']['import'])
            cls.flow_map.update(flow_config['flow-definitions'])
            cls.build_flows(flow_config['flows'])

    @classmethod
    def build_main_flow(cls, name, items):
        tasks = []
        for item in items:
            if item.get('task'):
                tasks.append(dict(task=signature(item.get('task'))))
            elif item.get('flow'):
                tasks.extend(cls.flow_map[item['flow']])
        entry_run = cls.build_entry_task()
        task_attributes = {
            'name': name,
            'run': entry_run,
            'task_list': tasks
        }
        cls.register_task(task_attributes, EntryTask)

    @classmethod
    def build_flows(cls, flows: dict):
        if flows is None:
            return {}
        for name, flow in flows.items():
            cls.build_entry_flow(name, flow)

    @classmethod
    def build_entry_flow(cls, name, flow):
        tasks = []
        if flow['type'] == 'task':
            pass
            # tasks.append(dict(task=signature(item.get('task'))))
        elif flow['type'] == 'flow':
            tasks.extend(cls.flow_map[flow['name']])
        entry_run = cls.build_entry_task()
        task_attributes = {
            'name': name,
            'run': entry_run,
            'task_list': tasks
        }
        cls.register_task(task_attributes, EntryTask)

    @classmethod
    def build_entry_task(cls):
        def entry_run(self, *args, **kwargs):
            if 'payload' in kwargs:
                payload = kwargs.get('payload')
            elif len(args) == 1:
                payload = args[0]
            elif len(args) == 2:
                payload = args[1]

            chained_tasks = []
            for task in self.task_list:
                task_name = task['name']
                condition = task.get('condition')
                task_options = dict(condition=condition, flow=self.name) \
                    if condition else dict(condition={}, flow=self.name)
                chained_tasks.append(signature(task_name, options=task_options))

            # chained_tasks.append(signature('common.process_end', immutable=True))
            main_chain = chain(chained_tasks)
            self.insert_to_chain(main_chain)
            return payload

        return entry_run

    @classmethod
    def register_task(cls, attributes, base_task=Task):
        task = type('Task', (base_task,), attributes)()
        app.tasks.register(task)
        return task

    @classmethod
    def parse_main_flows(cls, attributes):
        if attributes is None:
            return
        for attr in attributes:
            flow_name = attr['name']
            cls.build_main_flow(flow_name, attr['flows'])

    @classmethod
    def parse_work_flows(cls, attributes):
        if attributes is None:
            return {}
        flows = dict()
        for attr in attributes:
            flow_name = attr['name']
            task_list = [signature(task) for task in attr['tasks']]
            flows[flow_name] = task_list
        return flows

    @classmethod
    def parse_tasks(cls, attributes: list):
        for attr in attributes:
            print(f'{config.WORKER_NAME}.tasks.{attr}')
            __import__(f'{config.WORKER_NAME}.tasks.{attr}')


class CustomCelery(Celery):
    pre_text = f'{config.WORKER_NAME}.tasks.'

    def gen_task_name(self, name, module):
        if module.startswith(self.pre_text):
            module = module[len(self.pre_text):]
        return super(CustomCelery, self).gen_task_name(name, module)


app = CustomCelery(config.WORKER_NAME)
app.config_from_object(config)
builder = FlowBuilder.init(os.path.dirname(os.path.abspath(__file__)))
