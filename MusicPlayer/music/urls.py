from django.urls import path,include
from django.conf.urls.static import static
from . import views

app_name = 'music'

urlpatterns = [
    path('',views.index, name="index")
]
