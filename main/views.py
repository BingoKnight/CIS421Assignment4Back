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

    # This function is called on a post request, it will be used to join new relations
    # and add them to the hash table
    def post(self, request, *args, **kwargs):
        r1 = request.data['r1']
        r2 = request.data['r2']
        join_index_r1 = int(request.data['join_index_r1'])
        join_index_r2 = int(request.data['join_index_r2'])

        relational_joins_list = []

        # if the relations do not have the same number of tuples they may not be joined
        if len(r1) != len(r2):
            return Response({'details': 'Relations must match in amount of tuples they contain'}, status=HTTP_400_BAD_REQUEST)

        # loop through each tuple sent by looping the tuples in r1 and r2 at the same time
        # to join them at the end of each iteration
        for i, (tuple_r1, tuple_r2) in enumerate(zip(r1, r2)):

            # ensure the value the two tuples are joined on is not blank or null
            if tuple_r1[join_index_r1].isspace() or \
                    tuple_r1[join_index_r1] is None or \
                    tuple_r1[join_index_r1].strip() == '':
                return Response({'details': 'Null or undefined values not allowed'}, status=HTTP_400_BAD_REQUEST)

            # create the hash id where the joined tuples will be stored
            hash_id = int(tuple_r1[join_index_r1]) % 10
            collisions = 0

            # loop through the buckets of the hash table if there is a collision
            # only loop a max of 10 times because there are only 10 buckets
            while JoinedRelationsModel.objects.filter(hash_id=hash_id).exists() and collisions < 10:
                collisions += 1
                hash_id = (hash_id + 1) % 10

            # if there is 10 collisions then the hash table must be full
            if collisions == 10:
                return Response({'details': 'Hash table is full'})

            # the values which the two tuples are to be joined on
            join_val_r1 = None
            join_val_r2 = None

            # check to see if the indices selected for the tuples fits within the length
            # of each tuple
            if not (join_index_r1 < len(tuple_r1) and join_index_r2 < len(tuple_r2)):
                return Response({'details': 'Join index is out of range in tuples at index ' + str(i)}, status=HTTP_400_BAD_REQUEST)
            else:
                join_val_r1 = int(tuple_r1[join_index_r1])
                join_val_r2 = int(tuple_r2[join_index_r2])

            # ensure that the values that have been selected to join the tuples match eachother
            if join_val_r1 != join_val_r2:
                return Response({'details': 'Join values do not match in tuple at index ' + str(i)}, status=HTTP_400_BAD_REQUEST)

            # remove the index for the values that were joined upon because it will be added to
            # the hash table as the joined_field and the others will fill in after
            tuple_r1_indices = [0, 1, 2, 3]
            tuple_r1_indices.remove(join_index_r1)

            tuple_r2_indices = [0, 1, 2, 3]
            tuple_r2_indices.remove(join_index_r2)

            # create a hash table record to be added but don't save it yet in case an error occurs
            # in following tuple joins
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

            # add hash table record to list of records to eventually be saved in table
            relational_joins_list.append(joined_relations)

            # if any of the values int the hash record are null or blank return an error
            if None in joined_relations.values():
                return Response({'details': 'Null or undefined values not allowed'}, status=HTTP_400_BAD_REQUEST)

        # loop through each hashed record to add it to the hash table
        for joined_relations in relational_joins_list:
            serializer = self.get_serializer(data=joined_relations)

            # use serializer validation to check if values are numbers
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

    # gets list of all records in hash table
    def get(self, *args, **kwargs):
        return Response(
            JoinedRelationsSerializer(
                JoinedRelationsModel.objects.all(),
                context=self.get_serializer_context(),
                many=True
            ).data
        )

    # clears out hash table
    def delete(self, *args, **kwargs):
        JoinedRelationsModel.objects.all().delete()
        return Response({'details': 'database was deleted'})
