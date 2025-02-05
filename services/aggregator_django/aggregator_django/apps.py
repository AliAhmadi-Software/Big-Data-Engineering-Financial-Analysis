from django.apps import AppConfig

class AggregatorDjangoConfig(AppConfig):
    name = 'aggregator_django'

    def ready(self):
        import aggregator_django.signals