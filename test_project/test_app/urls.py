from django.urls import path

from test_app.views import TestDetailView
from test_app.views import TestListView


urlpatterns = [
    path(r'tests/<int:pk>/', TestDetailView.as_view(), name='test_detail'),
    path(r'tests/', TestListView.as_view(), name='test_list'),
]

from test_app.views import TestViewSet
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register(r'rf/tests', TestViewSet)

urlpatterns += router.urls
