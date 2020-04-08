from django.db import models


class JoinedRelationsModel(models.Model):
    hash_id = models.IntegerField(primary_key=True)
    joined_field = models.IntegerField()
    field1 = models.IntegerField()
    field2 = models.IntegerField()
    field3 = models.IntegerField()
    field4 = models.IntegerField()
    field5 = models.IntegerField()
    field6 = models.IntegerField()

    def __str__(self):
        return str(self.joined_field)