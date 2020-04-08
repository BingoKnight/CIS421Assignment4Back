from rest_framework.serializers import ModelSerializer
from .models import JoinedRelationsModel


class JoinedRelationsSerializer(ModelSerializer):

    class Meta:
        model = JoinedRelationsModel
        fields = '__all__'

    def create(self, validated_data):
        return JoinedRelationsModel.objects.create(**validated_data)
