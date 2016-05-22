from .models import PipeContext


def run_pipeline(key, purpose=None, attempt=0):
    context = PipeContext()
    context.evaluate(key, purpose, attempt)
