from django.db import models


FILL_VALUE=-999999999


class Tag(models.Model):
    name = models.CharField(max_length=128, unique=True)


class Bin(models.Model):
    "represents metadata about an IFCB bin"
    # bin's permanent identifier (e.g., D20190102T1234_IFCB927)
    pid = models.CharField(max_length=64, unique=True)
    # the parsed bin timestamp
    timestamp = models.DateTimeField('bin timestamp', db_index=True)
    # the parsed instrument number
    instrument = models.IntegerField(default=0)
    # provenance information
    import_timestamp = models.DateTimeField(auto_now_add=True)
    # spatiotemporal information
    sample_time = models.DateTimeField('sample time', db_index=True)
    lat = models.FloatField(null=True)
    lon = models.FloatField(null=True)
    depth = models.FloatField(null=True)
    # metadata about sampling
    sample_type = models.CharField(max_length=128, blank=True)
    # for at-sea samples we need a code identifying the cruise
    cruise = models.CharField(max_length=128, blank=True)
    # for casts we need cast and niskin number
    # casts sometimes have numbers like "2a"
    cast = models.CharField(max_length=64, blank=True)
    # niskin numbers should always be integers
    niskin = models.IntegerField(null=True)
    # tagging
    tags = models.ManyToManyField(Tag)
    
    def add_tag(self, tag_name):
        tag, _ = Tag.objects.get_or_create(name=tag_name)
        self.tags.add(tag)
        
    def remove_tag(self, tag_name):
        tag = Tag.objects.get(name=tag_name)
        self.tags.remove(tag)
