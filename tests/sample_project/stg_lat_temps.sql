select c.lat, c.lon, w.date, w.prcp
from sample_project.weather w, sample_project.cities c where w.city = c.name
