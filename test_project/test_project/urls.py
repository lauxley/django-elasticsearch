from django.urls import path, include
from django.http import HttpResponseNotFound

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = [
    path(r'', include('test_app.urls')),
]

def custom404(request, exception):
    return HttpResponseNotFound(status=404)

handler404 = 'test_project.urls.custom404'
