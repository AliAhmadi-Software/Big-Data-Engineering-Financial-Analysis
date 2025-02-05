# services/aggregator_django/aggregator/urls.py

from django.urls import path
from .views import AggregationView, StockSummaryView, MultiStockSummaryView

urlpatterns = [
    path('aggregate/', AggregationView.as_view(), name='aggregate'),
    path('summarize/', StockSummaryView.as_view(), name='summarize'),
    path('summarize/multiple/', MultiStockSummaryView.as_view(), name='multi-summarize'),
]
