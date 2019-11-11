from CeleryFlow.worker import app, EventTask


@app.task(base=EventTask)
def task_a(payload):
    return payload


@app.task(base=EventTask)
def task_b(payload):
    return payload


@app.task(base=EventTask)
def task_c(payload):
    return payload
