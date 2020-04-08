from django.urls import path
from .views import MainAPIView

urlpatterns = [
    path('', MainAPIView.as_view()),
]
