from rest_framework import generics
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, HTTP_200_OK
from .serializer import JoinedRelationsSerializer
from .models import JoinedRelationsModel


class MainAPIView(generics.GenericAPIView):

    '''
        10 buckets allowed in this hash table

        position joined tuples by has function f(x) = x mod 10

        Relations can have any number of tuples

        request object:
        {
            r1 : [
                [ <tuple of ints> ],
                [ <tuple of ints> ],
                [ <tuple of ints> ]
            ],
            r2 : [
                [ <tuple of ints> ],
                [ <tuple of ints> ],
                [ <tuple of ints> ],
                [ <tuple of ints> ]
            ],
            join_index_r1 : <R1 -> tuple -> index>
            join_index_r2 : <R2 -> tuple -> index>
        }
    '''

    serializer_class = JoinedRelationsSerializer

    def post(self, request, *args, **kwargs):
        r1 = request.data['r1']
        r2 = request.data['r2']
        join_index_r1 = int(request.data['join_index_r1'])
        join_index_r2 = int(request.data['join_index_r2'])

        print(type(join_index_r1))
        print(type(join_index_r2))

        relational_joins_list = []

        if len(r1) != len(r2):
            return Response({'details': 'Relations must match in amount of tuples they contain'}, status=HTTP_400_BAD_REQUEST)

        for i, (tuple_r1, tuple_r2) in enumerate(zip(r1, r2)):

            print(tuple_r1[join_index_r1].strip())
            print(int('0'))
            print(tuple_r1[join_index_r1].strip().isspace())
            print(tuple_r1[join_index_r1] is None)
            if tuple_r1[join_index_r1].isspace() or \
                    tuple_r1[join_index_r1] is None or \
                    tuple_r1[join_index_r1].strip() == '':
                return Response({'details': 'Null or undefined values not allowed'}, status=HTTP_400_BAD_REQUEST)

            hash_id = int(tuple_r1[join_index_r1]) % 10
            print(hash_id)
            collisions = 0

            while JoinedRelationsModel.objects.filter(hash_id=hash_id).exists() and collisions < 10:
                collisions += 1
                hash_id = (hash_id + 1) % 10

            if collisions == 10:
                return Response({'details': 'Hash table is full'})

            join_val_r1 = None
            join_val_r2 = None

            if not (join_index_r1 < len(tuple_r1) and join_index_r2 < len(tuple_r2)):
                return Response({'details': 'Join index is out of range in tuples at index ' + str(i)}, status=HTTP_400_BAD_REQUEST)
            else:
                join_val_r1 = int(tuple_r1[join_index_r1])
                join_val_r2 = int(tuple_r2[join_index_r2])

            if join_val_r1 != join_val_r2:
                return Response({'details': 'Join values do not match in tuple at index ' + str(i)}, status=HTTP_400_BAD_REQUEST)

            tuple_r1_indices = [0, 1, 2, 3]
            tuple_r1_indices.remove(join_index_r1)

            tuple_r2_indices = [0, 1, 2, 3]
            tuple_r2_indices.remove(join_index_r2)

            print(hash_id)

            joined_relations = {
                'hash_id': hash_id,
                'joined_field': join_val_r1,
                'field1': tuple_r1[tuple_r1_indices[0]],
                'field2': tuple_r1[tuple_r1_indices[1]],
                'field3': tuple_r1[tuple_r1_indices[2]],
                'field4': tuple_r2[tuple_r2_indices[0]],
                'field5': tuple_r2[tuple_r2_indices[1]],
                'field6': tuple_r2[tuple_r2_indices[2]],
            }

            relational_joins_list.append(joined_relations)

            if None in joined_relations.values():
                return Response({'details': 'Null or undefined values not allowed'}, status=HTTP_400_BAD_REQUEST)

        for joined_relations in relational_joins_list:
            serializer = self.get_serializer(data=joined_relations)
            if not serializer.is_valid():
                return Response({'details': 'Valid integers are required'}, status=HTTP_400_BAD_REQUEST)
            serializer.save()

        return Response(
            JoinedRelationsSerializer(
                JoinedRelationsModel.objects.all(),
                context=self.get_serializer_context(),
                many=True
            ).data
        )

    def get(self, *args, **kwargs):
        return Response(
            JoinedRelationsSerializer(
                JoinedRelationsModel.objects.all(),
                context=self.get_serializer_context(),
                many=True
            ).data
        )

    def delete(self, *args, **kwargs):
        JoinedRelationsModel.objects.all().delete()
        return Response({'details': 'database was deleted'})
