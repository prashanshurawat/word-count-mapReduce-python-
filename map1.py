import grpc
from concurrent import futures
import MapReduce_pb2
import MapReduce_pb2_grpc

Inputs = []


class map(MapReduce_pb2_grpc.map):
    def inputSplits(self, request, context, **kwargs):
        print("______________________________________________________")
        print("Split Received: \n", request.input)
        Inputs.extend(request.input)
        response = MapReduce_pb2.input_response()
        response.response = "Input Splits Received by map 1"
        print("______________________________________________________")
        return response

server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
MapReduce_pb2_grpc.add_mapServicer_to_server(map(), server)
server.add_insecure_port("[::]:50052")
server.start()
print("MAP 1 STARTED AT PORT 50052 ")
server.wait_for_termination(timeout=15)
print("50052 TERMINATED")


print("Input splits received ", Inputs)
print("______________________________________________________")

counts_map = {}
for word in Inputs:
    if str(word).lower() in counts_map:
        counts_map[str(word).lower()] = counts_map[str(word).lower()] + 1
    else:
        counts_map[str(word).lower()] = 1

print(counts_map)
print("______________________________________________________")

r = 2
with open("map1/p0.txt", "w+") as file:
    for word, count in counts_map.items():
        if len(word) % r == 0:
            file.write(str(word).lower() + ":" + str(count) + "\n")
    file.close()

with open("map1/p1.txt", "w+") as file:
    for word, count in counts_map.items():
        if len(word) % r == 1:
            file.write(str(word).lower() + ":" + str(count) + "\n")
    file.close()

