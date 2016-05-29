from django.test import TestCase
from .models import Pipe

# Create your tests here.


class AddOne(Pipe):

    def run(self, number):
        return number + 1


class AddTwo(Pipe):

    def run(self, number):
        result = yield AddOne(number)
        yield AddOne(result)
